import ResourcesService from "components/services/ResourcesService";
import { AutoMockService, MockedValue } from "test/mocks/services/AutoMockService";
import { ResourcesStubs } from "test/stubs/ResourcesStubs";

export default class MockResourcesService extends AutoMockService<ResourcesService> {
    constructor() {
        super(new ResourcesService());
    }

    withValidateNameCommand(dto?: MockedValue<Raven.Client.Util.NameValidation>) {
        return this.mockResolvedValue(this.mocks.validateName, dto, ResourcesStubs.validValidateName());
    }

    withDatabaseLocation(dto?: Raven.Server.Web.Studio.DataDirectoryResult) {
        return this.mockResolvedValue(this.mocks.getDatabaseLocation, dto, ResourcesStubs.databaseLocation());
    }

    withFolderPathOptions_ServerLocal(dto?: Raven.Server.Web.Studio.FolderPathOptions) {
        return this.mockResolvedValue(
            this.mocks.getFolderPathOptions_ServerLocal,
            dto,
            ResourcesStubs.folderPathOptions_ServerLocal()
        );
    }

    withRestorePoints_Local(dto?: any) {
        return this.mockResolvedValue(this.mocks.getRestorePoints_Local, dto, ResourcesStubs.restorePoints_Local());
    }
}
