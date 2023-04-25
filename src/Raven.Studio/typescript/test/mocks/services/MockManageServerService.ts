import ManageServerService from "components/services/ManageServerService";
import { AutoMockService, MockedValue } from "./AutoMockService";
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import { ManageServerStubs } from "test/stubs/ManageServerStubs";

export default class MockManageServerService extends AutoMockService<ManageServerService> {
    constructor() {
        super(new ManageServerService());
    }

    withGetGlobalClientConfiguration(dto?: MockedValue<ClientConfiguration>) {
        return this.mockResolvedValue(
            this.mocks.getGlobalClientConfiguration,
            dto,
            ManageServerStubs.getSampleClientGlobalConfiguration()
        );
    }

    withGetDatabaseClientConfiguration(dto?: MockedValue<ClientConfiguration>) {
        return this.mockResolvedValue(
            this.mocks.getClientConfiguration,
            dto,
            ManageServerStubs.getSampleClientDatabaseConfiguration()
        );
    }
}
