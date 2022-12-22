import { useEffect, useState } from "react";
import { MockDatabaseManager } from "test/mocks/hooks/MockDatabaseManager";

const mockManager = new MockDatabaseManager();

export function useDatabaseManager() {
    const [state, setState] = useState(mockManager.state());

    useEffect(() => {
        const sub = mockManager.state.subscribe(setState);

        return () => sub.dispose();
    }, []);

    return {
        databases: state.databasesLocal,
    };
}
