import { ActiveDatabaseGuard } from "components/common/ActiveDatabaseGuard";
import { act, rtlRender } from "test/rtlTestUtils";
import { mockStore } from "test/mocks/store/MockStore";

const activateDatabase = (name: string) =>
    act(() => {
        mockStore.databases.withActiveDatabase((db) => {
            db.name = name;
        });
    });

describe("ActiveDatabaseGuard", () => {
    it("renders the page only while the database it was created for is active", () => {
        const { screen } = rtlRender(
            <ActiveDatabaseGuard databaseName="db1">
                <div>Page of db1</div>
            </ActiveDatabaseGuard>
        );

        expect(screen.queryByText("Page of db1")).not.toBeInTheDocument();

        activateDatabase("db1");

        expect(screen.getByText("Page of db1")).toBeInTheDocument();

        activateDatabase("db2");

        expect(screen.queryByText("Page of db1")).not.toBeInTheDocument();
    });
});
