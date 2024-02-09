import getDatabaseLocationCommand from "commands/resources/getDatabaseLocationCommand";
import getFolderPathOptionsCommand from "commands/resources/getFolderPathOptionsCommand";
import getRestorePointsCommand from "commands/resources/getRestorePointsCommand";
import validateNameCommand from "commands/resources/validateNameCommand";

export default class ResourcesService {
    async validateName(...args: ConstructorParameters<typeof validateNameCommand>) {
        return new validateNameCommand(...args).execute();
    }

    async getFolderPathOptions_ServerLocal(...args: Parameters<typeof getFolderPathOptionsCommand.forServerLocal>) {
        return getFolderPathOptionsCommand.forServerLocal(...args).execute();
    }

    async getFolderPathOptions_CloudBackup(...args: Parameters<typeof getFolderPathOptionsCommand.forCloudBackup>) {
        return getFolderPathOptionsCommand.forCloudBackup(...args).execute();
    }

    async getDatabaseLocation(...args: ConstructorParameters<typeof getDatabaseLocationCommand>) {
        console.log("kalczur getDatabaseLocation args", args);
        return new getDatabaseLocationCommand(...args).execute();
    }

    async getRestorePoints_Local(...args: Parameters<typeof getRestorePointsCommand.forServerLocal>) {
        return getRestorePointsCommand.forServerLocal(...args).execute();
    }
}
