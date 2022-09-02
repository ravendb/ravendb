import React, { useCallback, useEffect, useMemo, useReducer, useState } from "react";
import { useServices } from "../../../hooks/useServices";
import { databasesStatsReducer, databasesStatsReducerInitializer, DatabasesStatsState } from "./DatabasesStatsReducer";
import { DatabasePanel } from "./DatabasePanel";
import { DatabasesToolbarActions } from "./DatabasesToolbarActions";
import { DatabasesFilter } from "./DatabasesFilter";
import { DatabasesCounter } from "./DatabasesCounter";
import { NoDatabases } from "./NoDatabases";
import { DatabaseFilterCriteria, DatabaseSharedInfo } from "../../../models/databases";
import { useChanges } from "hooks/useChanges";

interface DatabasesPageProps {
    activeDatabase: string;
}

function filterDatabases(stats: DatabasesStatsState, criteria: DatabaseFilterCriteria) {
    if (criteria.searchText) {
        return stats.databases.filter((x) => x.name.toLowerCase().includes(criteria.searchText.toLowerCase()));
    }

    return stats.databases;
}

export function DatabasesPage(props: DatabasesPageProps) {
    //TODO: highlight active database

    const [stats, dispatch] = useReducer(databasesStatsReducer, null, databasesStatsReducerInitializer);

    const [filter, setFilter] = useState<DatabaseFilterCriteria>(() => ({
        searchText: "",
    }));

    const { serverNotifications } = useChanges();

    const [selectedDatabases, setSelectedDatabases] = useState<string[]>([]);

    const { databasesService } = useServices();

    const filteredDatabases = useMemo(() => {
        //TODO: filter and sort databases
        //TODO: update selection if needed
        return filterDatabases(stats, filter);
    }, [filter, stats]);

    const fetchDatabases = useCallback(async () => {
        const stats = await databasesService.getDatabases();

        dispatch({
            type: "StatsLoaded",
            stats,
        });
    }, []);

    const toggleSelectAll = useCallback(() => {
        const selectedCount = selectedDatabases.length;

        if (selectedCount > 0) {
            setSelectedDatabases([]);
        } else {
            setSelectedDatabases(filteredDatabases.map((x) => x.name));
        }
    }, [selectedDatabases, filteredDatabases]);

    const databasesSelectionState = useMemo<checkbox>(() => {
        const selectedCount = selectedDatabases.length;
        const dbsCount = filteredDatabases.length;
        if (stats.databases && dbsCount === selectedCount) {
            return "checked";
        }

        if (selectedCount > 0) {
            return "some_checked";
        }

        return "unchecked";
    }, [filteredDatabases, selectedDatabases]);

    const toggleSelection = (db: DatabaseSharedInfo) => {
        if (selectedDatabases.includes(db.name)) {
            setSelectedDatabases((s) => s.filter((x) => x !== db.name));
        } else {
            setSelectedDatabases((s) => s.concat(db.name));
        }
    };

    useEffect(() => {
        fetchDatabases();
    }, []);

    useEffect(() => {
        if (serverNotifications) {
            const sub = serverNotifications.watchAllDatabaseChanges(() => fetchDatabases());

            return () => sub.off();
        }
    }, [serverNotifications]);

    return (
        <div>
            <div className="flex-header">
                <div className="databasesToolbar">
                    <DatabasesToolbarActions />
                    <DatabasesFilter
                        filter={filter}
                        setFilter={setFilter}
                        selectionState={databasesSelectionState}
                        toggleSelectAll={toggleSelectAll}
                    />
                </div>
            </div>
            <div
                className="flex-grow scroll js-scroll-container"
                data-bind="if: databases().sortedDatabases().length, visible: databases().sortedDatabases().length"
            >
                <DatabasesCounter />
                <div>
                    {filteredDatabases.map((db) => (
                        <DatabasePanel
                            key={db.name}
                            selected={selectedDatabases.includes(db.name)}
                            toggleSelection={() => toggleSelection(db)}
                            db={db}
                        />
                    ))}

                    {!stats.databases.length && <NoDatabases />}
                </div>
            </div>
        </div>
    );
}
