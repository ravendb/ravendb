import { serverWideOnlyEntry, databaseEntry } from "models/database/settings/databaseSettingsModels"

export type IndexingDatabaseSetting = "Indexing.Analyzers.Default" | "Indexing.Analyzers.Exact.Default" | "Indexing.Analyzers.Search.Default"

export type IndexingDatabaseSettingsType = Record<IndexingDatabaseSetting, serverWideOnlyEntry | databaseEntry<string | number>>