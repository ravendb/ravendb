/// <reference path="../../../typings/tsd.d.ts" />

import saveDatabaseLockModeCommand from "commands/resources/saveDatabaseLockModeCommand";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { DatabaseSharedInfo } from "../models/databases";
import database from "models/resources/database";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import getEssentialDatabaseStatsCommand from "commands/resources/getEssentialDatabaseStatsCommand";
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import getDatabaseDetailedStatsCommand from "commands/resources/getDatabaseDetailedStatsCommand";
import deleteDatabaseFromNodeCommand from "commands/resources/deleteDatabaseFromNodeCommand";
import toggleDynamicNodeAssignmentCommand from "commands/database/dbGroup/toggleDynamicNodeAssignmentCommand";
import reorderNodesInDatabaseGroupCommand = require("commands/database/dbGroup/reorderNodesInDatabaseGroupCommand");
import shard from "models/resources/shard";
import deleteOrchestratorFromNodeCommand from "commands/resources/deleteOrchestratorFromNodeCommand";
import DatabaseInfo = Raven.Client.ServerWide.Operations.DatabaseInfo;
import getDatabaseCommand from "commands/resources/getDatabaseCommand";

export default class DatabasesService {
    async setLockMode(db: DatabaseSharedInfo, newLockMode: DatabaseLockMode) {
        return new saveDatabaseLockModeCommand([db], newLockMode).execute();
    }

    async getEssentialStats(db: database): Promise<EssentialDatabaseStatistics> {
        return new getEssentialDatabaseStatsCommand(db).execute();
    }

    async getDetailedStats(db: database, location: databaseLocationSpecifier): Promise<DetailedDatabaseStatistics> {
        return new getDatabaseDetailedStatsCommand(db, location).execute();
    }

    async deleteDatabaseFromNode(db: database, nodes: string[], hardDelete: boolean) {
        return new deleteDatabaseFromNodeCommand(db, nodes, hardDelete).execute();
    }

    async deleteOrchestratorFromNode(db: database, node: string) {
        return new deleteOrchestratorFromNodeCommand(db, node).execute();
    }

    async toggleDynamicNodeAssignment(db: database, enabled: boolean) {
        return new toggleDynamicNodeAssignmentCommand(db.name, enabled).execute();
    }

    async reorderNodesInGroup(db: database, tagsOrder: string[], fixOrder: boolean) {
        return new reorderNodesInDatabaseGroupCommand(db.name, tagsOrder, fixOrder).execute();
    }

    async reorderShardsInGroup(db: shard, tagsOrder: string[]) {
        return new reorderNodesInDatabaseGroupCommand(db.name, tagsOrder, false).execute();
    }
}
