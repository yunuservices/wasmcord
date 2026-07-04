pub mod models;

use anyhow::Result;
use models::*;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Sqlite, SqlitePool};

#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
}

impl Database {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await?;

        Self::migrate(&pool).await?;

        Ok(Self { pool })
    }

    async fn migrate(pool: &SqlitePool) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS guild_config (
                guild_id  INTEGER PRIMARY KEY,
                prefix    TEXT NOT NULL DEFAULT '!'
            )",
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS plugin_data (
                guild_id    INTEGER NOT NULL,
                plugin_name TEXT NOT NULL,
                key         TEXT NOT NULL,
                value       BLOB NOT NULL,
                PRIMARY KEY (guild_id, plugin_name, key)
            )",
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn get_guild_config(&self, guild_id: u64) -> Result<Option<GuildConfig>> {
        let result = sqlx::query_as::<Sqlite, (i64, String)>(
            "SELECT guild_id, prefix FROM guild_config WHERE guild_id = ?",
        )
        .bind(guild_id as i64)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.map(|(id, prefix)| GuildConfig {
            guild_id: id,
            prefix,
        }))
    }

    pub async fn set_guild_config(&self, config: &GuildConfig) -> Result<()> {
        sqlx::query(
            "INSERT INTO guild_config (guild_id, prefix)
             VALUES (?, ?)
             ON CONFLICT(guild_id) DO UPDATE SET prefix = excluded.prefix",
        )
        .bind(config.guild_id)
        .bind(&config.prefix)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_plugin_data(
        &self,
        guild_id: u64,
        plugin_name: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>> {
        let result = sqlx::query_scalar::<Sqlite, Vec<u8>>(
            "SELECT value FROM plugin_data WHERE guild_id = ? AND plugin_name = ? AND key = ?",
        )
        .bind(guild_id as i64)
        .bind(plugin_name)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result)
    }

    pub async fn set_plugin_data(
        &self,
        guild_id: u64,
        plugin_name: &str,
        key: &str,
        value: &[u8],
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO plugin_data (guild_id, plugin_name, key, value)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(guild_id, plugin_name, key) DO UPDATE SET value = excluded.value",
        )
        .bind(guild_id as i64)
        .bind(plugin_name)
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
