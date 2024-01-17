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

    withLocalFolderPathOptions(dto?: Raven.Server.Web.Studio.FolderPathOptions) {
        return this.mockResolvedValue(
            this.mocks.getLocalFolderPathOptions,
            dto,
            ResourcesStubs.localFolderPathOptions()
        );
    }
}
