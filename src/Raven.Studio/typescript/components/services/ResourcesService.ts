import getDatabaseLocationCommand from "commands/resources/getDatabaseLocationCommand";
import getFolderPathOptionsCommand from "commands/resources/getFolderPathOptionsCommand";
import validateNameCommand from "commands/resources/validateNameCommand";
import database from "models/resources/database";

export default class ResourcesService {
    async validateName(type: Raven.Server.Web.Studio.StudioTasksHandler.ItemType, name: string, dataPath?: string) {
        return new validateNameCommand(type, name, dataPath).execute();
    }

    async getLocalFolderPathOptions(path: string, isBackupFolder: boolean, db: database) {
        return getFolderPathOptionsCommand.forServerLocal(path, isBackupFolder, null, db).execute();
    }

    async getDatabaseLocation(dbName: string, path: string) {
        return new getDatabaseLocationCommand(dbName, path).execute();
    }
}
