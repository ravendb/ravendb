import models = require("models/database/settings/databaseSettingsModels");

export type IndexingDatabaseSetting = "Indexing.Analyzers.Default" | "Indexing.Analyzers.Exact.Default" | "Indexing.Analyzers.Search.Default"

export type IndexingDatabaseSettingsType = Record<IndexingDatabaseSetting, models.serverWideOnlyEntry | models.databaseEntry<string | number>>