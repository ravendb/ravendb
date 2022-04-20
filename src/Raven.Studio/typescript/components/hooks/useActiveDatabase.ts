import { useEffect, useState } from "react";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import database from "models/resources/database";

export function useActiveDatabase() {
    const [db, setDb] = useState<database>();

    useEffect(() => {
        const activeDatabaseSubscription = activeDatabaseTracker.default.database.subscribe(setDb);

        return () => activeDatabaseSubscription.dispose();
    }, []);

    return {
        db,
    };
}
