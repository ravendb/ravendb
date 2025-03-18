import getCloudBackupCredentialsFromLinkCommand from "commands/resources/getCloudBackupCredentialsFromLinkCommand";
import getDatabaseLocationCommand from "commands/resources/getDatabaseLocationCommand";
import getFolderPathOptionsCommand from "commands/resources/getFolderPathOptionsCommand";
import getRestorePointsCommand from "commands/resources/getRestorePointsCommand";
import sendFeedbackCommand from "commands/resources/sendFeedbackCommand";
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
        return new getDatabaseLocationCommand(...args).execute();
    }

    async getRestorePoints_Local(...args: Parameters<typeof getRestorePointsCommand.forServerLocal>) {
        return getRestorePointsCommand.forServerLocal(...args).execute();
    }

    async getRestorePoints_S3Backup(...args: Parameters<typeof getRestorePointsCommand.forS3Backup>) {
        return getRestorePointsCommand.forS3Backup(...args).execute();
    }

    async getRestorePoints_AzureBackup(...args: Parameters<typeof getRestorePointsCommand.forAzureBackup>) {
        return getRestorePointsCommand.forAzureBackup(...args).execute();
    }

    async getRestorePoints_GoogleCloudBackup(...args: Parameters<typeof getRestorePointsCommand.forGoogleCloudBackup>) {
        return getRestorePointsCommand.forGoogleCloudBackup(...args).execute();
    }

    async getCloudBackupCredentialsFromLink(
        ...args: ConstructorParameters<typeof getCloudBackupCredentialsFromLinkCommand>
    ) {
        return new getCloudBackupCredentialsFromLinkCommand(...args).execute();
    }

    async sendFeedback(...args: ConstructorParameters<typeof sendFeedbackCommand>) {
        return new sendFeedbackCommand(...args).execute();
    }
}
