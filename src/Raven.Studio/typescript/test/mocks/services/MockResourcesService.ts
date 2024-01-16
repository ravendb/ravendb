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
}
