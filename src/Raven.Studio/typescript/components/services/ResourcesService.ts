import validateNameCommand from "commands/resources/validateNameCommand";

export default class ResourcesService {
    async validateName(type: Raven.Server.Web.Studio.StudioTasksHandler.ItemType, name: string, dataPath?: string) {
        return new validateNameCommand(type, name, dataPath).execute();
    }
}
