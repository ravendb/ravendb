import { useAppSelector } from "components/store";
import { selectDatabasesSummary } from "components/common/shell/databasesSlice";
import React from "react";
import { shallowEqual } from "react-redux";

export function DatabasesCounter(): JSX.Element {
    const databasesSummary = useAppSelector(selectDatabasesSummary, shallowEqual);

    return (
        <div className="database-counter on-base-background">
            <span className="px-1">
                <strong>{databasesSummary.count}</strong> Databases
            </span>
            {databasesSummary.online > 0 && (
                <span className="px-1">
                    <strong className="text-success">{databasesSummary.online}</strong> Online
                </span>
            )}
            {databasesSummary.error > 0 && (
                <span className="px-1">
                    <strong className="text-danger">{databasesSummary.error}</strong> Errored
                </span>
            )}
            {databasesSummary.disabled > 0 && (
                <span className="px-1">
                    <strong className="text-warning">{databasesSummary.disabled}</strong> Disabled
                </span>
            )}
            {databasesSummary.offline > 0 && (
                <span className="px-1">
                    <strong className="text-muted">{databasesSummary.offline}</strong> Offline
                </span>
            )}
        </div>
    );
}
