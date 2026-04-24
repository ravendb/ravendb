import getGlobalClientConfigurationCommand from "commands/resources/getGlobalClientConfigurationCommand";
import saveGlobalClientConfigurationCommand = require("commands/resources/saveGlobalClientConfigurationCommand");
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import getClientConfigurationCommand = require("commands/resources/getClientConfigurationCommand");
import saveClientConfigurationCommand = require("commands/resources/saveClientConfigurationCommand");
import adminJsScriptCommand = require("commands/maintenance/adminJsScriptCommand");
import getServerWideCustomAnalyzersCommand = require("commands/serverWide/analyzers/getServerWideCustomAnalyzersCommand");
import deleteServerWideCustomAnalyzerCommand = require("commands/serverWide/analyzers/deleteServerWideCustomAnalyzerCommand");
import getServerWideCustomSortersCommand = require("commands/serverWide/sorters/getServerWideCustomSortersCommand");
import deleteServerWideCustomSorterCommand = require("commands/serverWide/sorters/deleteServerWideCustomSorterCommand");
import testPeriodicBackupCredentialsCommand = require("commands/serverWide/testPeriodicBackupCredentialsCommand");
import saveServerWideCustomSorterCommand = require("commands/serverWide/sorters/saveServerWideCustomSorterCommand");
import saveServerWideCustomAnalyzerCommand from "commands/serverWide/analyzers/saveServerWideCustomAnalyzerCommand";
import getServerSettingsCommand from "commands/maintenance/getServerSettingsCommand";
import getClusterLogCommand from "commands/database/cluster/getClusterLogCommand";
import getClusterLogEntryCommand from "commands/database/cluster/getClusterLogEntryCommand";
import removeEntryFromLogCommand from "commands/database/cluster/removeEntryFromLogCommand";
import getAdminLogsConfigurationCommand = require("commands/maintenance/getAdminLogsConfigurationCommand");
import getTrafficWatchConfigurationCommand = require("commands/maintenance/getTrafficWatchConfigurationCommand");
import getAdminLogsEventListenerConfigurationCommand = require("commands/maintenance/getAdminLogsEventListenerConfigurationCommand");
import saveAdminLogsConfigurationCommand = require("commands/maintenance/saveAdminLogsConfigurationCommand");
import saveAdminLogsEventListenerConfigurationCommand = require("commands/maintenance/saveAdminLogsEventListenerConfigurationCommand");
import saveTrafficWatchConfigurationCommand = require("commands/maintenance/saveTrafficWatchConfigurationCommand");
import getCertificatesCommand = require("commands/auth/getCertificatesCommand");
import getAdminStatsCommand = require("commands/resources/getAdminStatsCommand");
import getServerCertificateRenewalDateCommand = require("commands/auth/getServerCertificateRenewalDateCommand");
import getServerCertificateSetupModeCommand = require("commands/auth/getServerCertificateSetupModeCommand");
import generateTwoFactorSecretCommand = require("commands/auth/generateTwoFactorSecretCommand");
import forceRenewServerCertificateCommand = require("commands/auth/forceRenewServerCertificateCommand");
import deleteCertificateCommand = require("commands/auth/deleteCertificateCommand");
import updateCertificateCommand = require("commands/auth/updateCertificateCommand");
import uploadCertificateCommand = require("commands/auth/uploadCertificateCommand");
import registerSsoServerCertCommand = require("commands/auth/registerSsoServerCertCommand");
import registerSsoUserEntryCommand = require("commands/auth/registerSsoUserEntryCommand");
import replaceClusterCertificateCommand = require("commands/auth/replaceClusterCertificateCommand");
import getClusterDomainsCommand = require("commands/auth/getClusterDomainsCommand");

export default class ManageServerService {
    async getGlobalClientConfiguration(): Promise<ClientConfiguration> {
        return new getGlobalClientConfigurationCommand().execute();
    }

    async saveGlobalClientConfiguration(dto: ClientConfiguration): Promise<void> {
        return new saveGlobalClientConfigurationCommand(dto).execute();
    }

    async getClientConfiguration(databaseName: string): Promise<ClientConfiguration> {
        return new getClientConfigurationCommand(databaseName).execute();
    }

    async saveClientConfiguration(dto: ClientConfiguration, databaseName: string): Promise<void> {
        return new saveClientConfigurationCommand(dto, databaseName).execute();
    }

    async runAdminJsScript(script: string, targetDatabaseName?: string): Promise<{ Result: any }> {
        return new adminJsScriptCommand(script, targetDatabaseName).execute();
    }

    async getServerWideCustomAnalyzers() {
        return new getServerWideCustomAnalyzersCommand().execute();
    }

    async deleteServerWideCustomAnalyzer(name: string) {
        return new deleteServerWideCustomAnalyzerCommand(name).execute();
    }

    async saveServerWideCustomAnalyzer(...args: ConstructorParameters<typeof saveServerWideCustomAnalyzerCommand>) {
        return new saveServerWideCustomAnalyzerCommand(...args).execute();
    }

    async getServerWideCustomSorters() {
        return new getServerWideCustomSortersCommand().execute();
    }

    async deleteServerWideCustomSorter(name: string) {
        return new deleteServerWideCustomSorterCommand(name).execute();
    }

    async saveServerWideCustomSorter(...args: ConstructorParameters<typeof saveServerWideCustomSorterCommand>) {
        return new saveServerWideCustomSorterCommand(...args).execute();
    }

    async testPeriodicBackupCredentials(
        type: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType,
        config: Raven.Client.Documents.Operations.Backups.BackupSettings
    ) {
        return new testPeriodicBackupCredentialsCommand(type, config).execute();
    }

    async getServerSettings() {
        return new getServerSettingsCommand().execute();
    }

    async getClusterLog(nodeTag: string, from: number, pageSize: number) {
        return new getClusterLogCommand(nodeTag, from, pageSize).execute();
    }

    async getClusterLogEntry(nodeTag: string, index: number) {
        return new getClusterLogEntryCommand(nodeTag, index).execute();
    }

    async removeClusterEntryLog(nodeTag: string, index: number) {
        return new removeEntryFromLogCommand(nodeTag, index).execute();
    }

    async getAdminLogsConfiguration() {
        return new getAdminLogsConfigurationCommand().execute();
    }

    async saveAdminLogsConfiguration(...args: ConstructorParameters<typeof saveAdminLogsConfigurationCommand>) {
        return new saveAdminLogsConfigurationCommand(...args).execute();
    }

    async getTrafficWatchConfiguration() {
        return new getTrafficWatchConfigurationCommand().execute();
    }

    async saveTrafficWatchConfiguration(...args: ConstructorParameters<typeof saveTrafficWatchConfigurationCommand>) {
        return new saveTrafficWatchConfigurationCommand(...args).execute();
    }

    async getEventListenerConfiguration() {
        return new getAdminLogsEventListenerConfigurationCommand().execute();
    }

    async saveEventListenerConfiguration(
        ...args: ConstructorParameters<typeof saveAdminLogsEventListenerConfigurationCommand>
    ) {
        return new saveAdminLogsEventListenerConfigurationCommand(...args).execute();
    }

    async getCertificates(...args: ConstructorParameters<typeof getCertificatesCommand>) {
        return new getCertificatesCommand(...args).execute();
    }

    async getAdminStats(...args: ConstructorParameters<typeof getAdminStatsCommand>) {
        return new getAdminStatsCommand(...args).execute();
    }

    async getServerCertificateRenewalDate() {
        return new getServerCertificateRenewalDateCommand().execute();
    }

    async getServerCertificateSetupMode() {
        return new getServerCertificateSetupModeCommand().execute();
    }

    async generateTwoFactorSecret() {
        return new generateTwoFactorSecretCommand().execute();
    }

    async uploadCertificate(...args: ConstructorParameters<typeof uploadCertificateCommand>) {
        return new uploadCertificateCommand(...args).execute();
    }

    async forceRenewServerCertificate() {
        return new forceRenewServerCertificateCommand().execute();
    }

    async deleteCertificate(...args: ConstructorParameters<typeof deleteCertificateCommand>) {
        return new deleteCertificateCommand(...args).execute();
    }

    async updateCertificate(...args: ConstructorParameters<typeof updateCertificateCommand>) {
        return new updateCertificateCommand(...args).execute();
    }

    async registerSsoServerCert(...args: ConstructorParameters<typeof registerSsoServerCertCommand>) {
        return new registerSsoServerCertCommand(...args).execute();
    }

    async registerSsoUserEntry(...args: ConstructorParameters<typeof registerSsoUserEntryCommand>) {
        return new registerSsoUserEntryCommand(...args).execute();
    }

    async replaceClusterCertificate(...args: ConstructorParameters<typeof replaceClusterCertificateCommand>) {
        return new replaceClusterCertificateCommand(...args).execute();
    }

    async getClusterDomains() {
        return new getClusterDomainsCommand().execute();
    }
}
