/// <reference path="../../../typings/tsd.d.ts" />

import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import saveIndexPriorityCommand from "commands/database/index/saveIndexPriorityCommand";
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
import toggleDisableIndexingCommand from "commands/database/index/toggleDisableIndexingCommand";
import getIndexMergeSuggestionsCommand = require("commands/database/index/getIndexMergeSuggestionsCommand");
import deleteIndexCommand = require("commands/database/index/deleteIndexCommand");
import getIndexesDefinitionsCommand = require("commands/database/index/getIndexesDefinitionsCommand");
import saveIndexDefinitionCommand = require("commands/database/index/saveIndexDefinitionsCommand");

export default class IndexesService {
    async getProgress(databaseName: string, location: databaseLocationSpecifier) {
        return new getIndexesProgressCommand(databaseName, location).execute();
    }

    async setLockMode(indexes: IndexSharedInfo[], lockMode: IndexLockMode, databaseName: string) {
        await new saveIndexLockModeCommand(indexes, lockMode, databaseName).execute();
    }

    async setPriority(index: IndexSharedInfo, priority: IndexPriority, databaseName: string) {
        await new saveIndexPriorityCommand(index.name, priority, databaseName).execute();
    }

    async getStats(databaseName: string, location: databaseLocationSpecifier): Promise<IndexStats[]> {
        return new getIndexesStatsCommand(databaseName, location).execute();
    }

    async getDefinitions(...args: ConstructorParameters<typeof getIndexesDefinitionsCommand>) {
        return new getIndexesDefinitionsCommand(...args).execute();
    }

    async saveDefinitions(...args: ConstructorParameters<typeof saveIndexDefinitionCommand>) {
        return new saveIndexDefinitionCommand(...args).execute();
    }

    async resetIndex(...args: ConstructorParameters<typeof resetIndexCommand>) {
        await new resetIndexCommand(...args).execute();
    }

    async pauseAllIndexes(databaseName: string, location: databaseLocationSpecifier) {
        await new togglePauseIndexingCommand(false, databaseName, null, location).execute();
    }

    async resumeAllIndexes(databaseName: string, location: databaseLocationSpecifier) {
        await new togglePauseIndexingCommand(true, databaseName, null, location).execute();
    }

    async disableAllIndexes(databaseName: string) {
        await new toggleDisableIndexingCommand(false, databaseName).execute();
    }

    async enableAllIndexes(databaseName: string) {
        await new toggleDisableIndexingCommand(true, databaseName).execute();
    }

    async enable(index: IndexSharedInfo, databaseName: string, location: databaseLocationSpecifier) {
        await new enableIndexCommand(index.name, databaseName, location).execute();
    }

    async disable(index: IndexSharedInfo, databaseName: string, location: databaseLocationSpecifier) {
        await new disableIndexCommand(index.name, databaseName, location).execute();
    }

    async pause(index: IndexSharedInfo, databaseName: string, location: databaseLocationSpecifier) {
        await new togglePauseIndexingCommand(false, databaseName, { name: index.name }, location).execute();
    }

    async resume(index: IndexSharedInfo, databaseName: string, location: databaseLocationSpecifier) {
        await new togglePauseIndexingCommand(true, databaseName, { name: index.name }, location).execute();
    }

    async openFaulty(index: IndexSharedInfo, databaseName: string, location: databaseLocationSpecifier) {
        await new openFaultyIndexCommand(index.name, databaseName, location).execute();
    }

    async forceReplace(indexName: string, databaseName: string, location: databaseLocationSpecifier) {
        await new forceIndexReplace(indexName, databaseName, location).execute();
    }

    async getIndexMergeSuggestions(databaseName: string) {
        return await new getIndexMergeSuggestionsCommand(databaseName).execute();
    }

    async deleteIndex(indexName: string, databaseName: string) {
        await new deleteIndexCommand(indexName, databaseName).execute();
    }
}
