import { ChangesContextProps } from "hooks/useChangesContext";

class MockChangesContext {
    get context(): ChangesContextProps {
        return {
            databaseNotifications: jest.fn(),
            serverNotifications: {
                watchAllDatabaseChanges: jest.fn(),

                //TODO:
            } as any,
        };
    }
}

export const mockChangesContext = new MockChangesContext();
