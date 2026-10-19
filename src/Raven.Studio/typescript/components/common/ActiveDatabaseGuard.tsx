import { EmptySet } from "components/common/EmptySet";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { PropsWithChildren, useRef } from "react";

interface ActiveDatabaseGuardProps {
    databaseName: string;
}

// After a database switch the Knockout router replaces the view with one created for the new database
export function ActiveDatabaseGuard({ databaseName, children }: PropsWithChildren<ActiveDatabaseGuardProps>) {
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const wasActiveRef = useRef(false);

    if (activeDatabaseName === databaseName) {
        wasActiveRef.current = true;
        return children;
    }

    if (activeDatabaseName === null && wasActiveRef.current) {
        return (
            <div className="content-padding">
                <EmptySet icon="database">
                    Database <strong>{databaseName}</strong> is no longer available
                </EmptySet>
            </div>
        );
    }

    return null;
}
