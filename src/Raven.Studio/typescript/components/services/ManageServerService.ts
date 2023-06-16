import getGlobalClientConfigurationCommand from "commands/resources/getGlobalClientConfigurationCommand";
import saveGlobalClientConfigurationCommand = require("commands/resources/saveGlobalClientConfigurationCommand");
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import getClientConfigurationCommand = require("commands/resources/getClientConfigurationCommand");
import saveClientConfigurationCommand = require("commands/resources/saveClientConfigurationCommand");
import database = require("models/resources/database");
import adminJsScriptCommand = require("commands/maintenance/adminJsScriptCommand");

export default class ManageServerService {
    async getGlobalClientConfiguration(): Promise<ClientConfiguration> {
        return new getGlobalClientConfigurationCommand().execute();
    }

    async saveGlobalClientConfiguration(dto: ClientConfiguration): Promise<void> {
        return new saveGlobalClientConfigurationCommand(dto).execute();
    }

    async getClientConfiguration(db: database): Promise<ClientConfiguration> {
        return new getClientConfigurationCommand(db).execute();
    }

    async saveClientConfiguration(dto: ClientConfiguration, db: database): Promise<void> {
        return new saveClientConfigurationCommand(dto, db).execute();
    }

    async runAdminJsScript(script: string, targetDatabaseName?: string): Promise<{ Result: any }> {
        return new adminJsScriptCommand(script, targetDatabaseName).execute();
    }
}
