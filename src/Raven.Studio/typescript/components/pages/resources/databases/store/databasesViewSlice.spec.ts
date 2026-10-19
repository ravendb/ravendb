import { createStoreConfiguration } from "components/store";
import { configureMockServices } from "components/hooks/useServices";
import { mockServices } from "test/mocks/services/MockServices";
import { databasesViewSliceInternal } from "components/pages/resources/databases/store/databasesViewSlice";
import StudioDatabasesState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabasesState;

const emptyState: StudioDatabasesState = { Databases: [], Orchestrators: [] };

function mockPendingDatabasesState() {
    configureMockServices(mockServices.context);
    const getDatabasesState = mockServices.databasesService.mock.getDatabasesState as jest.Mock;
    getDatabasesState.mockReset();

    let resolveState: (state: StudioDatabasesState) => void;
    getDatabasesState
        .mockReturnValueOnce(new Promise((resolve) => (resolveState = resolve)))
        .mockResolvedValue(emptyState);

    return { getDatabasesState, resolveState: () => resolveState(emptyState) };
}

describe("databasesViewSlice", () => {
    it("skips fetching databases state for a node that is still loading", () => {
        const { getDatabasesState } = mockPendingDatabasesState();
        const store = createStoreConfiguration();

        store.dispatch(databasesViewSliceInternal.loadNodeDatabases("A"));
        store.dispatch(databasesViewSliceInternal.loadNodeDatabases("A"));

        expect(getDatabasesState).toHaveBeenCalledTimes(1);
    });

    it("refetches databases state once when it was requested while loading", async () => {
        const { getDatabasesState, resolveState } = mockPendingDatabasesState();
        const store = createStoreConfiguration();

        const first = store.dispatch(databasesViewSliceInternal.loadNodeDatabases("A"));
        store.dispatch(databasesViewSliceInternal.loadNodeDatabases("A"));
        store.dispatch(databasesViewSliceInternal.loadNodeDatabases("A"));

        resolveState();
        await first;

        expect(getDatabasesState).toHaveBeenCalledTimes(2);
        expect(
            databasesViewSliceInternal.databaseDetailedLoadStatusSelectors.selectById(
                store.getState().databasesView.detailedLoadStatus,
                "A"
            ).status
        ).toBe("success");
    });
});
