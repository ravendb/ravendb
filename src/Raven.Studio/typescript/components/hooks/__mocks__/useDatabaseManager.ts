import { useCallback, useEffect, useState } from "react";
import { MockDatabaseManager } from "test/mocks/hooks/MockDatabaseManager";

const mockManager = new MockDatabaseManager();

export function useDatabaseManager() {
    const [state, setState] = useState(mockManager.state());

    const findByName = useCallback((name: string) => {
        return mockManager.state().databasesLocal.find((x) => x.name.toLocaleLowerCase() === name.toLocaleLowerCase());
    }, []);

    useEffect(() => {
        const sub = mockManager.state.subscribe(setState);

        return () => sub.dispose();
    }, []);

    return {
        databases: state.databasesLocal,
        findByName,
    };
}
