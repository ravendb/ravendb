import { ActiveDatabaseGuard } from "components/common/ActiveDatabaseGuard";
import { databaseActions } from "components/common/shell/databaseSliceActions";
import { globalDispatch } from "components/storeCompat";
import { act, rtlRender } from "test/rtlTestUtils";
import { mockStore } from "test/mocks/store/MockStore";

const activateDatabase = (name: string) =>
    act(() => {
        mockStore.databases.withActiveDatabase((db) => {
            db.name = name;
        });
    });

const clearActiveDatabase = () =>
    act(() => {
        globalDispatch(databaseActions.activeDatabaseChanged(null));
    });

const renderGuard = () =>
    rtlRender(
        <ActiveDatabaseGuard databaseName="db1">
            <div>Page of db1</div>
        </ActiveDatabaseGuard>
    );

describe("ActiveDatabaseGuard", () => {
    it("renders the page only while the database it was created for is active", () => {
        const { screen } = renderGuard();

        expect(screen.queryByText("Page of db1")).not.toBeInTheDocument();
        expect(screen.queryByText(/is no longer available/)).not.toBeInTheDocument();

        activateDatabase("db1");

        expect(screen.getByText("Page of db1")).toBeInTheDocument();

        activateDatabase("db2");

        expect(screen.queryByText("Page of db1")).not.toBeInTheDocument();
        expect(screen.queryByText(/is no longer available/)).not.toBeInTheDocument();
    });

    it("shows that the database is no longer available when it stops being active", () => {
        const { screen } = renderGuard();

        activateDatabase("db1");
        clearActiveDatabase();

        expect(screen.queryByText("Page of db1")).not.toBeInTheDocument();
        expect(screen.getByText(/is no longer available/)).toHaveTextContent("Database db1 is no longer available");
    });
});
