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
}
