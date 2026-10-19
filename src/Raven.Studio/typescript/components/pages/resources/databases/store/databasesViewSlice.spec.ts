import { createStoreConfiguration } from "components/store";
import { configureMockServices } from "components/hooks/useServices";
import { mockServices } from "test/mocks/services/MockServices";
import { databasesViewSliceInternal } from "components/pages/resources/databases/store/databasesViewSlice";
import StudioDatabasesState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabasesState;

describe("databasesViewSlice", () => {
    it("skips fetching databases state for a node that is still loading", async () => {
        configureMockServices(mockServices.context);
        const getDatabasesState = mockServices.databasesService.mock.getDatabasesState as jest.Mock;
        getDatabasesState.mockReset();

        let resolveState: (state: StudioDatabasesState) => void;
        getDatabasesState.mockReturnValue(new Promise((resolve) => (resolveState = resolve)));

        const store = createStoreConfiguration();
        const first = store.dispatch(databasesViewSliceInternal.fetchDatabases("A"));
        const second = store.dispatch(databasesViewSliceInternal.fetchDatabases("A"));

        expect(getDatabasesState).toHaveBeenCalledTimes(1);

        resolveState({ Databases: [], Orchestrators: [] });
        await Promise.all([first, second]);
        await store.dispatch(databasesViewSliceInternal.fetchDatabases("A"));

        expect(getDatabasesState).toHaveBeenCalledTimes(2);
    });
});
