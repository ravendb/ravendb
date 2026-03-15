/// <reference path="../../../typings/tsd.d.ts" />

import saveDatabaseLockModeCommand from "commands/resources/saveDatabaseLockModeCommand";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import getEssentialDatabaseStatsCommand from "commands/resources/getEssentialDatabaseStatsCommand";
import getDatabaseDetailedStatsCommand from "commands/resources/getDatabaseDetailedStatsCommand";
import deleteDatabaseFromNodeCommand from "commands/resources/deleteDatabaseFromNodeCommand";
import toggleDynamicNodeAssignmentCommand from "commands/database/dbGroup/toggleDynamicNodeAssignmentCommand";
import reorderNodesInDatabaseGroupCommand = require("commands/database/dbGroup/reorderNodesInDatabaseGroupCommand");
import deleteOrchestratorFromNodeCommand from "commands/resources/deleteOrchestratorFromNodeCommand";
import deleteDatabaseCommand from "commands/resources/deleteDatabaseCommand";
import toggleDatabaseCommand from "commands/resources/toggleDatabaseCommand";
import getDatabasesStateForStudioCommand from "commands/resources/getDatabasesStateForStudioCommand";
import getDatabaseStateForStudioCommand from "commands/resources/getDatabaseStateForStudioCommand";
import restartDatabaseCommand = require("commands/resources/restartDatabaseCommand");
import getDatabaseStudioConfigurationCommand = require("commands/resources/getDatabaseStudioConfigurationCommand");
import StudioConfiguration = Raven.Client.Documents.Operations.Configuration.StudioConfiguration;
import saveDatabaseStudioConfigurationCommand = require("commands/resources/saveDatabaseStudioConfigurationCommand");
import getNextOperationIdCommand = require("commands/database/studio/getNextOperationIdCommand");
import killOperationCommand = require("commands/operations/killOperationCommand");
import getRefreshConfigurationCommand = require("commands/database/documents/getRefreshConfigurationCommand");
import saveRefreshConfigurationCommand = require("commands/database/documents/saveRefreshConfigurationCommand");
import RefreshConfiguration = Raven.Client.Documents.Operations.Refresh.RefreshConfiguration;
import ExpirationConfiguration = Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration;
import saveExpirationConfigurationCommand = require("commands/database/documents/saveExpirationConfigurationCommand");
import getExpirationConfigurationCommand = require("commands/database/documents/getExpirationConfigurationCommand");
import DataArchivalConfiguration = Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration;
import getDataArchivalConfigurationCommand from "commands/database/documents/getDataArchivalConfigurationCommand";
import saveDataArchivalConfigurationCommand from "commands/database/documents/saveDataArchivalConfigurationCommand";
import getTombstonesStateCommand = require("commands/database/debug/getTombstonesStateCommand");
import forceTombstonesCleanupCommand = require("commands/database/debug/forceTombstonesCleanupCommand");
import RevisionsConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration;
import RevisionsCollectionConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration;
import getRevisionsForConflictsConfigurationCommand = require("commands/database/documents/getRevisionsForConflictsConfigurationCommand");
import getRevisionsConfigurationCommand = require("commands/database/documents/getRevisionsConfigurationCommand");
import saveRevisionsConfigurationCommand = require("commands/database/documents/saveRevisionsConfigurationCommand");
import saveRevisionsForConflictsConfigurationCommand = require("commands/database/documents/saveRevisionsForConflictsConfigurationCommand");
import enforceRevisionsConfigurationCommand = require("commands/database/settings/enforceRevisionsConfigurationCommand");
import getCustomSortersCommand = require("commands/database/settings/getCustomSortersCommand");
import deleteCustomSorterCommand = require("commands/database/settings/deleteCustomSorterCommand");
import deleteCustomAnalyzerCommand = require("commands/database/settings/deleteCustomAnalyzerCommand");
import getCustomAnalyzersCommand = require("commands/database/settings/getCustomAnalyzersCommand");
import getDocumentsCompressionConfigurationCommand = require("commands/database/documents/getDocumentsCompressionConfigurationCommand");
import saveDocumentsCompressionCommand = require("commands/database/documents/saveDocumentsCompressionCommand");
import promoteDatabaseNodeCommand = require("commands/database/debug/promoteDatabaseNodeCommand");
import revertRevisionsCommand = require("commands/database/documents/revertRevisionsCommand");
import getConflictSolverConfigurationCommand = require("commands/database/documents/getConflictSolverConfigurationCommand");
import getDatabaseRecordCommand = require("commands/resources/getDatabaseRecordCommand");
import saveDatabaseRecordCommand = require("commands/resources/saveDatabaseRecordCommand");
import saveConflictSolverConfigurationCommand = require("commands/database/documents/saveConflictSolverConfigurationCommand");
import saveCustomSorterCommand = require("commands/database/settings/saveCustomSorterCommand");
import queryCommand = require("commands/database/query/queryCommand");
import getIntegrationsPostgreSqlSupportCommand = require("commands/database/settings/getIntegrationsPostgreSqlSupportCommand");
import getIntegrationsPostgreSqlCredentialsCommand = require("commands/database/settings/getIntegrationsPostgreSqlCredentialsCommand");
import saveIntegrationsPostgreSqlCredentialsCommand = require("commands/database/settings/saveIntegrationsPostgreSqlCredentialsCommand");
import deleteIntegrationsPostgreSqlCredentialsCommand = require("commands/database/settings/deleteIntegrationsPostgreSqlCredentialsCommand");
import generateSecretCommand = require("commands/database/secrets/generateSecretCommand");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import saveUnusedDatabaseIDsCommand = require("commands/database/settings/saveUnusedDatabaseIDsCommand");
import { createDatabaseCommand } from "commands/resources/createDatabaseCommand";
import { restoreDatabaseFromBackupCommand } from "commands/resources/restoreDatabaseFromBackupCommand";
import distributeSecretCommand = require("commands/database/secrets/distributeSecretCommand");
import saveCustomAnalyzerCommand from "commands/database/settings/saveCustomAnalyzerCommand";
import getDocumentsPreviewCommand = require("commands/database/documents/getDocumentsPreviewCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import getIdentitiesCommand from "commands/database/identities/getIdentitiesCommand";
import seedIdentityCommand from "commands/database/identities/seedIdentityCommand";
import getRevisionsBinCleanerConfigurationCommand from "commands/database/settings/getRevisionsBinCleanerConfigurationCommand";
import saveRevisionsBinCleanerConfigurationCommand from "commands/database/settings/saveRevisionsBinCleanerConfigurationCommand";
import deleteDocumentsCommand from "commands/database/documents/deleteDocumentsCommand";
import deleteCollectionCommand from "commands/database/documents/deleteCollectionCommand";
import getDatabaseSettingsCommand = require("commands/database/settings/getDatabaseSettingsCommand");
import getRevisionsPreviewCommand from "commands/database/documents/getRevisionsPreviewCommand";
import deleteRevisionsForDocumentsCommand = require("commands/database/documents/deleteRevisionsForDocumentsCommand");
import getRevisionsIdsCommand from "commands/database/documents/getRevisionsIdsCommand";
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import getDocumentsByIDPrefixCommand = require("commands/database/documents/getDocumentsByIDPrefixCommand");
import getRemoteAttachmentsConfigurationCommand from "commands/database/settings/getRemoteAttachmentsConfigurationCommand";
import saveRemoteAttachmentsConfigurationCommand from "commands/database/settings/saveRemoteAttachmentsConfigurationCommand";
import getSchemaValidationCommand from "commands/database/settings/getSchemaValidationCommand";
import saveSchemaValidationCommand from "commands/database/settings/saveSchemaValidationCommand";
import validateSchemaCommand from "commands/database/settings/validateSchemaCommand";
import getRemoteAttachmentsDestinationsCommand from "commands/database/settings/getRemoteAttachmentsDestinationsCommand";

export default class DatabasesService {
    async setLockMode(databaseNames: string[], newLockMode: DatabaseLockMode) {
        return new saveDatabaseLockModeCommand(databaseNames, newLockMode).execute();
    }

    async toggle(databaseNames: string[], enable: boolean) {
        return new toggleDatabaseCommand(databaseNames, enable).execute();
    }

    async deleteDatabase(toDelete: string[], hardDelete: boolean) {
        return new deleteDatabaseCommand(toDelete, hardDelete).execute();
    }

    async getEssentialStats(databaseName: string) {
        return new getEssentialDatabaseStatsCommand(databaseName).execute();
    }

    async getDatabasesState(targetNodeTag: string) {
        return new getDatabasesStateForStudioCommand(targetNodeTag).execute();
    }

    async getDatabaseState(targetNodeTag: string, databaseName: string) {
        return new getDatabaseStateForStudioCommand(targetNodeTag, databaseName).execute();
    }

    async getDetailedStats(databaseName: string, location: databaseLocationSpecifier) {
        return new getDatabaseDetailedStatsCommand(databaseName, location).execute();
    }

    async deleteDatabaseFromNode(databaseName: string, nodes: string[], hardDelete: boolean) {
        return new deleteDatabaseFromNodeCommand(databaseName, nodes, hardDelete).execute();
    }

    async deleteOrchestratorFromNode(databaseName: string, node: string) {
        return new deleteOrchestratorFromNodeCommand(databaseName, node).execute();
    }

    async toggleDynamicNodeAssignment(databaseName: string, enabled: boolean) {
        return new toggleDynamicNodeAssignmentCommand(databaseName, enabled).execute();
    }

    async reorderNodesInGroup(databaseName: string, tagsOrder: string[], fixOrder: boolean) {
        return new reorderNodesInDatabaseGroupCommand(databaseName, tagsOrder, fixOrder).execute();
    }

    async restartDatabase(databaseName: string, location: databaseLocationSpecifier) {
        return new restartDatabaseCommand(databaseName, location).execute();
    }

    async getDatabaseStudioConfiguration(databaseName: string) {
        return new getDatabaseStudioConfigurationCommand(databaseName).execute();
    }

    async saveDatabaseStudioConfiguration(dto: Partial<StudioConfiguration>, databaseName: string) {
        return new saveDatabaseStudioConfigurationCommand(dto, databaseName).execute();
    }

    async getNextOperationId(databaseName: string) {
        return new getNextOperationIdCommand(databaseName).execute();
    }

    async killOperation(databaseName: string, taskId: number) {
        return new killOperationCommand(databaseName, taskId).execute();
    }

    async getRefreshConfiguration(databaseName: string) {
        return new getRefreshConfigurationCommand(databaseName).execute();
    }

    async saveRefreshConfiguration(databaseName: string, dto: RefreshConfiguration) {
        return new saveRefreshConfigurationCommand(databaseName, dto).execute();
    }

    async getDataArchivalConfiguration(databaseName: string) {
        return new getDataArchivalConfigurationCommand(databaseName).execute();
    }

    async saveDataArchivalConfiguration(databaseName: string, dto: DataArchivalConfiguration): Promise<any> {
        return new saveDataArchivalConfigurationCommand(databaseName, dto).execute();
    }

    async getExpirationConfiguration(databaseName: string) {
        return new getExpirationConfigurationCommand(databaseName).execute();
    }

    async saveExpirationConfiguration(databaseName: string, dto: ExpirationConfiguration) {
        return new saveExpirationConfigurationCommand(databaseName, dto).execute();
    }

    async getTombstonesState(databaseName: string, location: databaseLocationSpecifier) {
        return new getTombstonesStateCommand(databaseName, location).execute();
    }

    async forceTombstonesCleanup(databaseName: string, location: databaseLocationSpecifier) {
        return new forceTombstonesCleanupCommand(databaseName, location).execute();
    }

    async getRevisionsForConflictsConfiguration(databaseName: string) {
        return new getRevisionsForConflictsConfigurationCommand(databaseName).execute();
    }

    async saveRevisionsForConflictsConfiguration(databaseName: string, dto: RevisionsCollectionConfiguration) {
        return new saveRevisionsForConflictsConfigurationCommand(databaseName, dto).execute();
    }

    async getRevisionsConfiguration(databaseName: string) {
        return new getRevisionsConfigurationCommand(databaseName).execute();
    }

    async saveRevisionsConfiguration(databaseName: string, dto: RevisionsConfiguration) {
        return new saveRevisionsConfigurationCommand(databaseName, dto).execute();
    }

    async getRevisionsBinCleanerConfiguration(
        ...args: ConstructorParameters<typeof getRevisionsBinCleanerConfigurationCommand>
    ) {
        return new getRevisionsBinCleanerConfigurationCommand(...args).execute();
    }

    async saveRevisionsBinCleanerConfiguration(
        ...args: ConstructorParameters<typeof saveRevisionsBinCleanerConfigurationCommand>
    ) {
        return new saveRevisionsBinCleanerConfigurationCommand(...args).execute();
    }

    async enforceRevisionsConfiguration(
        databaseName: string,
        includeForceCreated = false,
        collections: string[] = null,
        maxOpsPerSecond: number = null
    ) {
        return new enforceRevisionsConfigurationCommand(databaseName, includeForceCreated, collections, maxOpsPerSecond).execute();
    }

    async revertRevisions(databaseName: string, dto: Raven.Server.Documents.Revisions.RevertRevisionsRequest) {
        return new revertRevisionsCommand(dto, databaseName).execute();
    }

    async getCustomAnalyzers(databaseName: string, getNamesOnly = false) {
        return new getCustomAnalyzersCommand(databaseName, getNamesOnly).execute();
    }

    async deleteCustomAnalyzer(databaseName: string, name: string) {
        return new deleteCustomAnalyzerCommand(databaseName, name).execute();
    }

    async saveCustomAnalyzer(...args: ConstructorParameters<typeof saveCustomAnalyzerCommand>) {
        return new saveCustomAnalyzerCommand(...args).execute();
    }

    async getCustomSorters(databaseName: string) {
        return new getCustomSortersCommand(databaseName).execute();
    }

    async deleteCustomSorter(databaseName: string, name: string) {
        return new deleteCustomSorterCommand(databaseName, name).execute();
    }

    async saveCustomSorter(...args: ConstructorParameters<typeof saveCustomSorterCommand>) {
        return new saveCustomSorterCommand(...args).execute();
    }

    async getDocumentsCompressionConfiguration(databaseName: string) {
        return new getDocumentsCompressionConfigurationCommand(databaseName).execute();
    }

    async saveDocumentsCompression(
        databaseName: string,
        dto: Raven.Client.ServerWide.DocumentsCompressionConfiguration
    ) {
        return new saveDocumentsCompressionCommand(databaseName, dto).execute();
    }

    async promoteDatabaseNode(databaseName: string, nodeTag: string) {
        return new promoteDatabaseNodeCommand(databaseName, nodeTag).execute();
    }

    async getConflictSolverConfiguration(databaseName: string) {
        return new getConflictSolverConfigurationCommand(databaseName).execute();
    }

    async saveConflictSolverConfiguration(databaseName: string, dto: Raven.Client.ServerWide.ConflictSolver) {
        return new saveConflictSolverConfigurationCommand(databaseName, dto).execute();
    }

    async getDatabaseRecord(databaseName: string, reportRefreshProgress = false) {
        return new getDatabaseRecordCommand(databaseName, reportRefreshProgress).execute();
    }

    async saveDatabaseRecord(databaseName: string, databaseRecord: documentDto, etag: number) {
        return new saveDatabaseRecordCommand(databaseName, databaseRecord, etag).execute();
    }

    async query(...args: ConstructorParameters<typeof queryCommand>) {
        return new queryCommand(...args).execute();
    }

    async getIntegrationsPostgreSqlSupport(databaseName: string) {
        return new getIntegrationsPostgreSqlSupportCommand(databaseName).execute();
    }

    async getIntegrationsPostgreSqlCredentials(databaseName: string) {
        return new getIntegrationsPostgreSqlCredentialsCommand(databaseName).execute();
    }

    async saveIntegrationsPostgreSqlCredentials(databaseName: string, username: string, password: string) {
        return new saveIntegrationsPostgreSqlCredentialsCommand(databaseName, username, password).execute();
    }

    async deleteIntegrationsPostgreSqlCredentials(databaseName: string, username: string) {
        return new deleteIntegrationsPostgreSqlCredentialsCommand(databaseName, username).execute();
    }

    async generateSecret() {
        return new generateSecretCommand().execute();
    }

    async getDatabaseStats(...args: ConstructorParameters<typeof getDatabaseStatsCommand>) {
        return new getDatabaseStatsCommand(...args).execute();
    }

    async saveUnusedDatabaseIDs(...args: ConstructorParameters<typeof saveUnusedDatabaseIDsCommand>) {
        return new saveUnusedDatabaseIDsCommand(...args).execute();
    }

    async createDatabase(...args: ConstructorParameters<typeof createDatabaseCommand>) {
        return new createDatabaseCommand(...args).execute();
    }

    async restoreDatabaseFromBackup(...args: ConstructorParameters<typeof restoreDatabaseFromBackupCommand>) {
        return new restoreDatabaseFromBackupCommand(...args).execute();
    }

    async distributeSecret(...args: ConstructorParameters<typeof distributeSecretCommand>) {
        return new distributeSecretCommand(...args).execute();
    }

    async getDocumentsPreview(...args: ConstructorParameters<typeof getDocumentsPreviewCommand>) {
        return new getDocumentsPreviewCommand(...args).execute();
    }

    async getDocumentsMetadataByIDPrefix(...args: ConstructorParameters<typeof getDocumentsMetadataByIDPrefixCommand>) {
        return new getDocumentsMetadataByIDPrefixCommand(...args).execute();
    }

    async getDocumentsByIDPrefix(...args: ConstructorParameters<typeof getDocumentsByIDPrefixCommand>) {
        return new getDocumentsByIDPrefixCommand(...args).execute();
    }

    async getIdentities(...args: ConstructorParameters<typeof getIdentitiesCommand>) {
        return new getIdentitiesCommand(...args).execute();
    }

    async seedIdentity(...args: ConstructorParameters<typeof seedIdentityCommand>) {
        return new seedIdentityCommand(...args).execute();
    }

    async deleteDocuments(...args: ConstructorParameters<typeof deleteDocumentsCommand>) {
        return new deleteDocumentsCommand(...args).execute();
    }

    async getRevisionsPreview(...args: ConstructorParameters<typeof getRevisionsPreviewCommand>) {
        return new getRevisionsPreviewCommand(...args).execute();
    }

    // Skip async to use JQueryPromise type
    deleteCollection(...args: ConstructorParameters<typeof deleteCollectionCommand>) {
        return new deleteCollectionCommand(...args).execute();
    }

    async getDatabaseSettings(...args: ConstructorParameters<typeof getDatabaseSettingsCommand>) {
        return new getDatabaseSettingsCommand(...args).execute();
    }

    async deleteRevisionsForDocuments(...args: ConstructorParameters<typeof deleteRevisionsForDocumentsCommand>) {
        return new deleteRevisionsForDocumentsCommand(...args).execute();
    }

    async getRevisionsIds(...args: ConstructorParameters<typeof getRevisionsIdsCommand>) {
        return new getRevisionsIdsCommand(...args).execute();
    }

    async getDocumentWithMetadata(...args: ConstructorParameters<typeof getDocumentWithMetadataCommand>) {
        return new getDocumentWithMetadataCommand(...args).execute();
    }

    async getRemoteAttachmentsConfiguration(
        ...args: ConstructorParameters<typeof getRemoteAttachmentsConfigurationCommand>
    ) {
        return new getRemoteAttachmentsConfigurationCommand(...args).execute();
    }

    async saveRemoteAttachmentsConfiguration(
        ...args: ConstructorParameters<typeof saveRemoteAttachmentsConfigurationCommand>
    ) {
        return new saveRemoteAttachmentsConfigurationCommand(...args).execute();
    }

    async getRemoteAttachmentsDestinations(
        ...args: ConstructorParameters<typeof getRemoteAttachmentsDestinationsCommand>
    ) {
        return new getRemoteAttachmentsDestinationsCommand(...args).execute();
    }

    async getSchemaValidation(...args: ConstructorParameters<typeof getSchemaValidationCommand>) {
        return new getSchemaValidationCommand(...args).execute();
    }

    async saveSchemaValidation(...args: ConstructorParameters<typeof saveSchemaValidationCommand>) {
        return new saveSchemaValidationCommand(...args).execute();
    }

    async validateSchema(...args: ConstructorParameters<typeof validateSchemaCommand>) {
        return new validateSchemaCommand(...args).execute();
    }
}
