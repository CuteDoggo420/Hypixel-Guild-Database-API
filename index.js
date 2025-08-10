const express = require("express");
const bodyParser = require("body-parser");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const axios = require("axios");
const PQueue = require("p-queue").default;
const dotenv = require('dotenv').config();

const app = express();
app.use(bodyParser.json());

const HYPIXEL_API_KEY = process.env.HYPIXEL_API_KEY;
if (!HYPIXEL_API_KEY) {
  console.error("Missing HYPIXEL_API_KEY environment variable");
  process.exit(1);
}
const stats = {
  totalGuildsTracked: 0, // will update on startup & new guild inserts
  guildsAddedTimestamps: [],
  hypixelApiRequestTimestamps: [],
  dbGuildRequestsTimestamps: [],
};

const DB_PATH = process.env.DB_PATH || path.join("/data", "hypixel_cache.db");

function countInWindow(timestamps, windowMs) {
  const cutoff = Date.now() - windowMs;
  while (timestamps.length && timestamps[0] < cutoff) {
    timestamps.shift();
  }
  return timestamps.length;
}


const db = new sqlite3.Database(DB_PATH, (err) => {
  if (err) return console.error(err.message);
  console.log("Connected to SQLite database.");
});
function ensureColumnExists(table, column, type) {
    db.all(`PRAGMA table_info(${table})`, (err, rows) => {
        if (err) return console.error(`Error checking table ${table}:`, err);
        const exists = rows.some(row => row.name === column);
        if (!exists) {
            db.run(`ALTER TABLE ${table} ADD COLUMN ${column} ${type}`, (err2) => {
                if (err2) console.error(`Error adding column ${column} to ${table}:`, err2);
                else console.log(`Added missing column ${column} to ${table}.`);
            });
        }
    });
}


// Run auto-migrations
ensureColumnExists("members", "highest_sb_level", "REAL DEFAULT NULL");
ensureColumnExists("members", "level_last_updated", "INTEGER DEFAULT NULL");
ensureColumnExists("guilds", "avg_sb_level", "REAL DEFAULT NULL");

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS guilds (
        guild_id TEXT PRIMARY KEY,
        name TEXT,
        tag TEXT,
        last_scan INTEGER,
        avg_sb_level REAL DEFAULT NULL
    )
`);

db.run(`
    CREATE TABLE IF NOT EXISTS members (
        uuid TEXT PRIMARY KEY,
        guild_id TEXT,
        rank TEXT,
        last_scan INTEGER,
        highest_sb_level REAL DEFAULT NULL,
        level_last_updated INTEGER DEFAULT NULL
    )
`);

});

db.get("SELECT COUNT(*) as count FROM guilds", (err, row) => {
  if (!err && row) stats.totalGuildsTracked = row.count;
});

function now() {
  return Math.floor(Date.now() / 1000);
}

const SEVEN_DAYS = 60*60;

// simple queue with concurrency 1 and intervalCap 60 per minute
const queue = new PQueue({
  concurrency: 1,
  interval: 60*1000,
  intervalCap: 1,
});

async function fetchGuildByUUID(uuid) {
  const url = `https://api.hypixel.net/v2/guild?player=${uuid}&key=${HYPIXEL_API_KEY}`;
  const response = await axios.get(url);
  stats.hypixelApiRequestTimestamps.push(Date.now());
  if (response.data.success && response.data.guild) {
    return response.data.guild;
  }
  return null;
}

function upsertGuild(guild) {
  return new Promise((resolve, reject) => {
    const lastScan = now();
    db.run(
      `INSERT INTO guilds (guild_id, name, tag, last_scan) VALUES (?, ?, ?, ?)
       ON CONFLICT(guild_id) DO UPDATE SET name=excluded.name, tag=excluded.tag, last_scan=excluded.last_scan`,
      [guild._id, guild.name, guild.tag || "", lastScan],
      (err) => {
        if (err) return reject(err);
        const stmt = db.prepare(
          `INSERT INTO members (uuid, guild_id, rank, last_scan) VALUES (?, ?, ?, ?)
           ON CONFLICT(uuid) DO UPDATE SET guild_id=excluded.guild_id, rank=excluded.rank, last_scan=excluded.last_scan`
        );

        const memberLastScan = now();
        for (const member of guild.members) {
          stmt.run([member.uuid.replace(/-/g, ""), guild._id, member.rank, memberLastScan]);
        }
        stats.totalGuildsTracked = stats.totalGuildsTracked + 1;
        stats.guildsAddedTimestamps.push(Date.now());
        stmt.finalize((err) => {
          if (err) return reject(err);
          resolve();
        });
      }
    );
  });
}

function removePlayerFromOldGuild(uuid, currentGuildId) {
  return new Promise((resolve, reject) => {
    db.get(`SELECT guild_id FROM members WHERE uuid = ?`, [uuid], (err, row) => {
      if (err) return reject(err);
      if (row && row.guild_id !== currentGuildId) {
        db.run(`DELETE FROM members WHERE uuid = ?`, [uuid], (err) => {
          if (err) return reject(err);
          resolve(true);
        });
      } else {
        resolve(false);
      }
    });
  });
}

async function scanPlayerGuild(uuid) {
  try {
    console.log(`Scanning guild for player ${uuid}...`);
    const guild = await fetchGuildByUUID(uuid);
    if (!guild) {
      db.run(
        `INSERT INTO players_no_guild (uuid, last_scan) VALUES (?, ?)
         ON CONFLICT(uuid) DO UPDATE SET last_scan=excluded.last_scan`,
        [uuid, now()]
      );
      console.log(`Player ${uuid} is not in a guild.`);
      return;
    }
    db.run(`DELETE FROM players_no_guild WHERE uuid = ?`, [uuid]);

    await removePlayerFromOldGuild(uuid, guild._id);

    await upsertGuild(guild);
    console.log(`Guild ${guild.name} (${guild._id}) updated.`);
  } catch (error) {
    console.error("Error scanning guild:", error.message);
  }
}

app.post("/player", (req, res) => {
  const { uuid } = req.body;
  if (!uuid) {
    return res.status(400).json({ error: "Missing uuid in request body." });
  }
  const cleanUUID = uuid.replace(/-/g, "");

  db.get(`SELECT * FROM members WHERE uuid = ?`, [cleanUUID], (err, member) => {
    if (err) {
      console.error(err);
      return res.status(500).json({ error: "Database error." });
    }
    if (member) {
      if (!member.last_scan || now() - member.last_scan > SEVEN_DAYS) {
        queue.add(() => scanPlayerGuild(cleanUUID));
        return res.json({ message: `Queued guild scan for guild ${member.guild_id}` });
      } else {
        return res.json({ message: "Guild recently scanned, no update needed." });
      }
    } else {
      db.get(`SELECT * FROM players_no_guild WHERE uuid = ?`, [cleanUUID], (err, player) => {
        if (err) {
          console.error(err);
          return res.status(500).json({ error: "Database error." });
        }
        if (player) {
          if (!player.last_scan || now() - player.last_scan > SEVEN_DAYS) {
            queue.add(() => scanPlayerGuild(cleanUUID));
            return res.json({ message: `Queued player scan for ${cleanUUID}` });
          } else {
            return res.json({ message: "Player recently scanned, no update needed." });
          }
        } else {
          db.run(
            `INSERT INTO players_no_guild (uuid, last_scan) VALUES (?, ?)`,
            [cleanUUID, 0],
            (err) => {
              if (err) {
                console.error(err);
                return res.status(500).json({ error: "Database insert error." });
              }
              queue.add(() => scanPlayerGuild(cleanUUID));
              return res.json({ message: `New player added and queued for scan.` });
            }
          );
        }
      });
    }
  });
});

app.get("/guild/:identifier", (req, res) => {
  const identifier = req.params.identifier;

  const isGuildId = /^[a-f0-9]{24}$/.test(identifier);

  const query = isGuildId
    ? "SELECT guild_id, name, tag, last_scan FROM guilds WHERE guild_id = ?"
    : "SELECT guild_id, name, tag, last_scan FROM guilds WHERE name = ?";

  db.get(query, [identifier], (err, guild) => {
    if (err) {
      console.error(err);
      return res.status(500).json({ error: "Database error" });
    }
    if (!guild) {
      return res.status(404).json({ error: "Guild not found" });
    }
    stats.dbGuildRequestsTimestamps.push(Date.now());
    db.all(
      "SELECT uuid, rank, last_scan FROM members WHERE guild_id = ?",
      [guild.guild_id],
      (err, members) => {
        if (err) {
          console.error(err);
          return res.status(500).json({ error: "Database error" });
        }

        res.json({
          guild_id: guild.guild_id,
          name: guild.name,
          tag: guild.tag,
          last_scan: guild.last_scan,
          members: members.map((m) => ({
            uuid: m.uuid,
            rank: m.rank,
          })),
        });
      }
    );
  });
});

app.get("/stats", (req, res) => {
    const now = Date.now();

    db.get(`SELECT COUNT(DISTINCT uuid) AS totalPlayers FROM members`, [], (err, playerRow) => {
        if (err) {
            console.error("Error counting players:", err);
            return res.status(500).json({ error: "Database error" });
        }
        

        const playersTracked = playerRow?.totalPlayers || 0;

        const statsData = {
          totalGuildsTracked: stats.totalGuildsTracked,
            hypixelApiRequestsLast5m: stats.hypixelApiRequestTimestamps.filter(ts => now - ts < 5 * 60 * 1000).length,
            guildsAddedInLast60s: stats.guildsAddedTimestamps.filter(ts => now - ts < 60 * 1000).length,
            playersTracked
        };

        res.json(statsData);
    });
});


app.get('/guilds', (req, res) => {
    const sql = `
        SELECT 
            g.name AS guildName, 
            COUNT(m.uuid) AS memberCount,
            g.average_level AS avgLevel,
            g.last_scan AS lastUpdated
        FROM guilds g
        LEFT JOIN members m ON g.guild_id = m.guild_id
        GROUP BY g.guild_id
        ORDER BY avgLevel DESC
    `;
    db.all(sql, [], (err, rows) => {
        if (err) {
            console.error("Error fetching guild list:", err);
            return res.status(500).json({ error: 'Database error' });
        }
        res.json(rows);
    });
});


async function processGuildLevels() {
    db.all(`
        SELECT DISTINCT m.uuid, m.guild_id
        FROM members m
        LEFT JOIN guilds g ON g.guild_id = m.guild_id
        ORDER BY m.level_last_updated ASC NULLS FIRST
        LIMIT 60
    `, async (err, rows) => {
        if (err) return console.error(err);

        for (const row of rows) {
            await updateMemberLevel(row.uuid);
            recalcGuildAverage(row.guild_id);
            await new Promise(r => setTimeout(r, 1000));
        }
    });
}

async function updateMemberLevel(uuid) {
    const nowTime = Date.now();
    const row = await new Promise((resolve, reject) => {
        db.get(`SELECT level_last_updated FROM members WHERE uuid = ?`, [uuid], (err, r) => {
            if (err) reject(err);
            else resolve(r);
        });
    });
    if (row && row.level_last_updated && (nowTime - row.level_last_updated) < 604800000) return; 

    const level = await fetchHighestSBLevel(uuid);
    if (level !== null) {
        db.run(`UPDATE members SET highest_sb_level = ?, level_last_updated = ? WHERE uuid = ?`,
            [level, nowTime, uuid]);
    }
}

async function fetchHighestSBLevel(uuid) {
    try {
        const profilesRes = await axios.get(`https://api.hypixel.net/v2/skyblock/profiles?key=${HYPIXEL_API_KEY}&uuid=${uuid}`);
        stats.hypixelApiRequestTimestamps.push(Date.now());

        if (!profilesRes.data.success || !profilesRes.data.profiles) return null;

        let highestLevel = 0;
        for (const profile of profilesRes.data.profiles) {
            if (profile?.members?.[uuid]?.leveling?.experience) {
                const xp = profile.members[uuid].leveling.experience;
                const level = xp / 100; 
                if (level > highestLevel) highestLevel = level;
            }
        }
        return highestLevel;
    } catch (err) {
        console.error(`Error fetching SB level for ${uuid}:`, err.message);
        return null;
    }
}


setInterval(() => {
    processGuildLevels(); // Uses the 60/minute slot to update levels
}, 60 * 1000); // every minute


const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`http://localhost:${PORT}`);
});
