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
import deleteDatabaseCommand from "commands/resources/deleteDatabaseCommand";
import toggleDatabaseCommand from "commands/resources/toggleDatabaseCommand";
import StudioDatabasesState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabasesState;
import getDatabasesStateForStudioCommand from "commands/resources/getDatabasesStateForStudioCommand";

export default class DatabasesService {
    async setLockMode(databases: DatabaseSharedInfo[], newLockMode: DatabaseLockMode) {
        return new saveDatabaseLockModeCommand(databases, newLockMode).execute();
    }

    async toggle(databases: DatabaseSharedInfo[], enable: boolean) {
        return new toggleDatabaseCommand(databases, enable).execute();
    }

    async deleteDatabase(toDelete: string[], hardDelete: boolean) {
        return new deleteDatabaseCommand(toDelete, hardDelete).execute();
    }

    async getEssentialStats(db: DatabaseSharedInfo): Promise<EssentialDatabaseStatistics> {
        return new getEssentialDatabaseStatsCommand(db).execute();
    }

    async getDatabasesState(targetNodeTag: string): Promise<StudioDatabasesState> {
        return new getDatabasesStateForStudioCommand(targetNodeTag).execute();
    }

    async getDetailedStats(
        db: DatabaseSharedInfo,
        location: databaseLocationSpecifier
    ): Promise<DetailedDatabaseStatistics> {
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
