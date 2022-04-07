import React, { useCallback, useEffect, useReducer } from "react";
import { useServices } from "../../../hooks/useServices";
import { databasesStatsReducer, databasesStatsReducerInitializer } from "./DatabasesStatsReducer";
import { DatabasePanel } from "./DatabasePanel";
import { DatabasesToolbarActions } from "./DatabasesToolbarActions";
import { DatabasesFilter } from "./DatabasesFilter";
import { DatabasesCounter } from "./DatabasesCounter";
import { NoDatabases } from "./NoDatabases";
import changesContext from "common/changesContext";

interface DatabasesPageProps {
}

export function DatabasesPage(props: DatabasesPageProps) {

    const { databasesService } = useServices();

    const [ stats, dispatch ] = useReducer(databasesStatsReducer, null, databasesStatsReducerInitializer);

    const fetchDatabases = useCallback(async () => {
        const stats = await databasesService.getDatabases();

        dispatch({
            type: "StatsLoaded",
            stats
        });
    }, []);

    useEffect(() => {
        fetchDatabases();
    }, []);
    
    useEffect(() => {
        const sub = changesContext.default.serverNotifications().watchAllDatabaseChanges(() => fetchDatabases());
        
        return () => sub.off();
    }, []);
    
    return (
        <div>
            <div className="flex-header">
                <div className="databasesToolbar">
                    <DatabasesToolbarActions />
                    <DatabasesFilter />
                </div>
            </div>
            <div className="flex-grow scroll js-scroll-container"
                 data-bind="if: databases().sortedDatabases().length, visible: databases().sortedDatabases().length">
                <DatabasesCounter />
                <div>
                    { stats.databases.map(db => (
                        <DatabasePanel 
                            key={db.name}
                            db={db} 
                        />
                    ))}

                    { !stats.databases.length && (
                        <NoDatabases />
                    )}
                </div>
            </div>
            
        </div>
    )
}
