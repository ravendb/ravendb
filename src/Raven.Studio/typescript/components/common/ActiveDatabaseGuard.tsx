import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { PropsWithChildren } from "react";

interface ActiveDatabaseGuardProps {
    databaseName: string;
}

// After a database switch the Knockout router replaces the view with one created for the new database
export function ActiveDatabaseGuard({ databaseName, children }: PropsWithChildren<ActiveDatabaseGuardProps>) {
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return activeDatabaseName === databaseName ? children : null;
}
