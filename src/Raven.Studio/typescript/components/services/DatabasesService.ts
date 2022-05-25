/// <reference path="../../../typings/tsd.d.ts" />

import getDatabasesCommand from "commands/resources/getDatabasesCommand";
import saveDatabaseLockModeCommand from "commands/resources/saveDatabaseLockModeCommand";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { DatabaseSharedInfo } from "../models/databases";
import database from "models/resources/database";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import getEssentialDatabaseStatsCommand from "commands/resources/getEssentialDatabaseStatsCommand";
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import getDatabaseDetailedStatsCommand from "commands/resources/getDatabaseDetailedStatsCommand";

export default class DatabasesService {
    async getDatabases() {
        return new getDatabasesCommand().execute();
    }

    async setLockMode(db: DatabaseSharedInfo, newLockMode: DatabaseLockMode) {
        return new saveDatabaseLockModeCommand([db], newLockMode).execute();
    }

    async getEssentialStats(db: database): Promise<EssentialDatabaseStatistics> {
        return new getEssentialDatabaseStatsCommand(db).execute();
    }

    async getDetailedStats(db: database, location: databaseLocationSpecifier): Promise<DetailedDatabaseStatistics> {
        return new getDatabaseDetailedStatsCommand(db, location).execute();
    }
}
