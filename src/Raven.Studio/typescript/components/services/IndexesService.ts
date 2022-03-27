/// <reference path="../../../typings/tsd.d.ts" />

import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import saveIndexPriorityCommand from "commands/database/index/saveIndexPriorityCommand";
import database from "models/resources/database";
import saveIndexLockModeCommand from "commands/database/index/saveIndexLockModeCommand";
import { IndexSharedInfo } from "../models/indexes";
import getIndexesStatsCommand from "commands/database/index/getIndexesStatsCommand";
import IndexUtils from "../utils/IndexUtils";
import resetIndexCommand from "commands/database/index/resetIndexCommand";
import enableIndexCommand from "commands/database/index/enableIndexCommand";
import disableIndexCommand from "commands/database/index/disableIndexCommand";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import togglePauseIndexingCommand from "commands/database/index/togglePauseIndexingCommand";
import getIndexesProgressCommand from "commands/database/index/getIndexesProgressCommand";
import openFaultyIndexCommand from "commands/database/index/openFaultyIndexCommand";
import forceIndexReplace from "commands/database/index/forceIndexReplace";

export default class IndexesService {
    
    async getProgress(db: database, location: databaseLocationSpecifier) {
        return new getIndexesProgressCommand(db, location)
            .execute();
    }
    
    async setLockMode(indexes: IndexSharedInfo[], lockMode: IndexLockMode, db: database) {
        await new saveIndexLockModeCommand(indexes, lockMode, db, IndexUtils.formatLockMode(lockMode))
            .execute();
    }
    
    async setPriority(index: IndexSharedInfo, priority: IndexPriority, db: database) {
        await new saveIndexPriorityCommand(index.name, priority, db)
            .execute();
    }
    
    async getStats(db: database, location: databaseLocationSpecifier): Promise<IndexStats[]> {
        const stats = await new getIndexesStatsCommand(db, location)
            .execute();
        
        return stats;
    }
    
    async resetIndex(index: IndexSharedInfo, db: database, location: databaseLocationSpecifier) {
        await new resetIndexCommand(index.name, db, location)
            .execute();
    } 
    
    async enable(index: IndexSharedInfo, db: database, location: databaseLocationSpecifier) {
        await new enableIndexCommand(index.name, db, location)
            .execute();
    }

    async disable(index: IndexSharedInfo, db: database, location: databaseLocationSpecifier) {
        await new disableIndexCommand(index.name, db, location)
            .execute();
    }

    async pause(indexes: IndexSharedInfo[], db: database, location: databaseLocationSpecifier) {
        await new togglePauseIndexingCommand(false, db, { name: indexes.map(x => x.name) }, location)
            .execute();
    }

    async resume(indexes: IndexSharedInfo[], db: database, location: databaseLocationSpecifier) {
        await new togglePauseIndexingCommand(true, db, { name: indexes.map(x => x.name) }, location)
            .execute();
    }
    
    async openFaulty(index: IndexSharedInfo, db: database, location: databaseLocationSpecifier) {
        await new openFaultyIndexCommand(index.name, db, location)
            .execute();
    }

    async forceReplace(name: string, database: database) {
        await new forceIndexReplace(name, database)
            .execute();
    }
}
