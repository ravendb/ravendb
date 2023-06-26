/// <reference path="../../../typings/tsd.d.ts" />

import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import saveIndexPriorityCommand from "commands/database/index/saveIndexPriorityCommand";
import database from "models/resources/database";
import saveIndexLockModeCommand from "commands/database/index/saveIndexLockModeCommand";
import { IndexSharedInfo } from "../models/indexes";
import getIndexesStatsCommand from "commands/database/index/getIndexesStatsCommand";
import resetIndexCommand from "commands/database/index/resetIndexCommand";
import enableIndexCommand from "commands/database/index/enableIndexCommand";
import disableIndexCommand from "commands/database/index/disableIndexCommand";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import togglePauseIndexingCommand from "commands/database/index/togglePauseIndexingCommand";
import getIndexesProgressCommand from "commands/database/index/getIndexesProgressCommand";
import openFaultyIndexCommand from "commands/database/index/openFaultyIndexCommand";
import forceIndexReplace from "commands/database/index/forceIndexReplace";
import { DatabaseSharedInfo } from "components/models/databases";
import toggleDisableIndexingCommand from "commands/database/index/toggleDisableIndexingCommand";

export default class IndexesService {
    async getProgress(db: database, location: databaseLocationSpecifier) {
        return new getIndexesProgressCommand(db, location).execute();
    }

    async setLockMode(indexes: IndexSharedInfo[], lockMode: IndexLockMode, db: database) {
        await new saveIndexLockModeCommand(indexes, lockMode, db).execute();
    }

    async setPriority(index: IndexSharedInfo, priority: IndexPriority, db: database) {
        await new saveIndexPriorityCommand(index.name, priority, db).execute();
    }

    async getStats(db: database | DatabaseSharedInfo, location: databaseLocationSpecifier): Promise<IndexStats[]> {
        return new getIndexesStatsCommand(db, location).execute();
    }

    async resetIndex(indexName: string, db: database, location: databaseLocationSpecifier) {
        await new resetIndexCommand(indexName, db, location).execute();
    }

    async pauseAllIndexes(db: DatabaseSharedInfo, location: databaseLocationSpecifier) {
        await new togglePauseIndexingCommand(false, db, null, location).execute();
    }

    async resumeAllIndexes(db: DatabaseSharedInfo, location: databaseLocationSpecifier) {
        await new togglePauseIndexingCommand(true, db, null, location).execute();
    }

    async disableAllIndexes(db: DatabaseSharedInfo) {
        await new toggleDisableIndexingCommand(false, db).execute();
    }

    async enableAllIndexes(db: DatabaseSharedInfo) {
        await new toggleDisableIndexingCommand(true, db).execute();
    }

    async enable(index: IndexSharedInfo, db: database, location: databaseLocationSpecifier) {
        await new enableIndexCommand(index.name, db, location).execute();
    }

    async disable(index: IndexSharedInfo, db: database, location: databaseLocationSpecifier) {
        await new disableIndexCommand(index.name, db, location).execute();
    }

    async pause(index: IndexSharedInfo, db: database, location: databaseLocationSpecifier) {
        await new togglePauseIndexingCommand(false, db, { name: index.name }, location).execute();
    }

    async resume(index: IndexSharedInfo, db: database, location: databaseLocationSpecifier) {
        await new togglePauseIndexingCommand(true, db, { name: index.name }, location).execute();
    }

    async openFaulty(index: IndexSharedInfo, db: database, location: databaseLocationSpecifier) {
        await new openFaultyIndexCommand(index.name, db, location).execute();
    }

    async forceReplace(indexName: string, database: database, location: databaseLocationSpecifier) {
        await new forceIndexReplace(indexName, database, location).execute();
    }
}
