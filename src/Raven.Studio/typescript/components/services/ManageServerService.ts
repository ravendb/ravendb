import getGlobalClientConfigurationCommand from "commands/resources/getGlobalClientConfigurationCommand";
import saveGlobalClientConfigurationCommand = require("commands/resources/saveGlobalClientConfigurationCommand");
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;

export default class ManageServerService {
    async getGlobalClientConfiguration(): Promise<ClientConfiguration> {
        return new getGlobalClientConfigurationCommand().execute();
    }

    async saveGlobalClientConfiguration(dto: ClientConfiguration): Promise<void> {
        return new saveGlobalClientConfigurationCommand(dto).execute();
    }
}
