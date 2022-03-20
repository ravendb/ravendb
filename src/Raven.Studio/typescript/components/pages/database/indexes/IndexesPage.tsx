import React, { useEffect, useMemo, useReducer, useState } from "react"
import database from "models/resources/database";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import {
    IndexFilterCriteria, IndexGroup,
    IndexSharedInfo, IndexStatus,
} from "../../../models/indexes";
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import { IndexPanel } from "./IndexPanel";
import appUrl from "common/appUrl";
import deleteIndexesConfirm from "viewmodels/database/indexes/deleteIndexesConfirm";
import app from "durandal/app";
import IndexFilter, { IndexFilterDescription } from "./IndexFilter";
import IndexGlobalIndexing from "./IndexGlobalIndexing";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import IndexToolbarActions from "./IndexToolbarActions";
import { useServices } from "../../../hooks/useServices";
import { indexesStatsReducer, indexesStatsReducerInitializer } from "./IndexesStatsReducer";
import collection from "models/database/documents/collection";
import IndexUtils from "../../../utils/IndexUtils";
import genUtils from "common/generalUtils";
import { shardingTodo } from "common/developmentHelper";

interface IndexesPageProps {
    database: database;
    reload: () => Promise<void>;
}

async function promptDeleteIndexes(db: database, indexes: IndexSharedInfo[]): Promise<void> {
    if (indexes.length > 0) {
        const deleteIndexesVm = new deleteIndexesConfirm(indexes, db);
        app.showBootstrapDialog(deleteIndexesVm);
        deleteIndexesVm.deleteTask
            .done((deleted: boolean) => {
                if (deleted) {
                    this.removeIndexesFromAllGroups(indexes); //todo
                }
            });
        await deleteIndexesVm.deleteTask;
    }
}


function NoIndexes() {
    const newIndexUrl = appUrl.forCurrentDatabase().newIndex();
    
    return (
        <div className="row">
            <div className="col-sm-8 col-sm-offset-2 col-lg-6 col-lg-offset-3">
                <i className="icon-xl icon-empty-set text-muted"/>
                <h2 className="text-center text-muted">No indexes have been created for this database.</h2>
                <p data-bind="requiredAccess: 'DatabaseReadWrite'" className="lead text-center text-muted">
                    Go ahead and <a href={newIndexUrl}>create one now</a>.</p>
            </div>
        </div>
    )
}

export const defaultFilterCriteria: IndexFilterCriteria = {
    status: ["Normal", "ErrorOrFaulty", "Stale", "Paused", "Disabled", "Idle", "RollingDeployment"],
    autoRefresh: true,
    showOnlyIndexesWithIndexingErrors: false,
    searchText: ""
};

function matchesAnyIndexStatus(index: IndexSharedInfo, status: IndexStatus[]): boolean {
    if (status.length === 0) {
        return false;
    }

    shardingTodo();
    /* TODO
    ADD : _.includes(status, "Stale") && this.isStale()
        || _.includes(status, "RollingDeployment") && this.rollingDeploymentInProgress()
     */
    return status.includes("Normal") && IndexUtils.isNormalState(index)
        || status.includes("ErrorOrFaulty") && (IndexUtils.isErrorState(index) || IndexUtils.isFaulty(index))
        || status.includes("Paused") && IndexUtils.isPausedState(index)
        || status.includes("Disabled") && IndexUtils.isDisabledState(index)
        || status.includes("Idle") && IndexUtils.isIdleState(index);
}

function indexMatchesFilter(index: IndexSharedInfo, filter: IndexFilterCriteria): boolean {
    const nameMatch = !filter.searchText || index.name.toLowerCase().includes(filter.searchText.toLowerCase());
    const statusMatch = matchesAnyIndexStatus(index, filter.status);
    const indexingErrorsMatch = true; //TODO:  !withIndexingErrorsOnly || (withIndexingErrorsOnly && !!this.errorsCount());

    return nameMatch && statusMatch && indexingErrorsMatch;
}

function groupAndFilterIndexStats(indexes: IndexSharedInfo[], collections: collection[], filter: IndexFilterCriteria) {
    const result = new Map<string, IndexGroup>();

    indexes.forEach(index => {
        if (!indexMatchesFilter(index, filter)) {
            return ;
        }

        const groupName = IndexUtils.getIndexGroupName(index, collections);
        if (!result.has(groupName)) {
            const group: IndexGroup = {
                name: groupName,
                indexes: []
            }
            result.set(groupName, group);
        }

        const group = result.get(groupName);
        group.indexes.push(index);
    });

    // sort groups
    const groups = Array.from(result.values());
    groups.sort((l, r) => genUtils.sortAlphaNumeric(l.name, r.name));

    groups.forEach(group => {
        group.indexes.sort((a, b) => genUtils.sortAlphaNumeric(a.name, b.name));
    });
    
    return groups;
}


export function IndexesPage(props: IndexesPageProps) {
    const { database } = props;
    const locations = database.getLocations();
    
    const { indexesService } = useServices();
    
    const [stats, dispatch] = useReducer(indexesStatsReducer, locations, indexesStatsReducerInitializer);
    
    const initialLocation = locations[0]; //TODO:
    
    const [filter, setFilter] = useState<IndexFilterCriteria>(defaultFilterCriteria);
    
    const groups = useMemo(() => {
        const collections = collectionsTracker.default.collections();
        return groupAndFilterIndexStats(stats.indexes, collections, filter);
    }, [stats, filter]);
    
    const [selectedIndexes, setSelectedIndexes] = useState<string[]>([]);
    
    const fetchStats = async (location: databaseLocationSpecifier) => {
        const stats = await indexesService.getStats(database, location);
        dispatch({
            type: "StatsLoaded",
            location,
            stats
        });
    };
    
    useEffect(() => {
        fetchStats(initialLocation);
    }, []);
    
    const setIndexPriority = async (index: IndexSharedInfo, priority: IndexPriority) => {
        await indexesService.setPriority(index, priority, database);

        dispatch({
            type: "SetPriority",
            priority,
            indexName: index.name
        });
    }
    
    const setIndexLockMode = async (index: IndexSharedInfo, lockMode: IndexLockMode) => {
        await indexesService.setLockMode([index], lockMode, database);
        
        dispatch({
            type: "SetLockMode",
            lockMode,
            indexName: index.name
        });
    }

    if (stats.indexes.length === 0) {
        return <NoIndexes />
    }

    const toggleSelection = (index: IndexSharedInfo) => {
        if (selectedIndexes.includes(index.name)) {
            setSelectedIndexes(s => s.filter(x => x !== index.name));
        } else {
            setSelectedIndexes(s => s.concat(index.name));
        }
    }
    
    const loadMissing = () => { //TODO: temp
        stats.indexes[0].nodesInfo.forEach(nodeInfo => {
            if (nodeInfo.status === "notLoaded") {
                fetchStats(nodeInfo.location);
            }
        })
    }
    
    return (
        <div className="flex-vertical absolute-fill">
            <div className="flex-header">
                {stats.indexes.length > 0 && (
                    <div className="clearfix toolbar">
                        <div className="pull-left">
                            <div className="form-inline">
                                <div className="checkbox checkbox-primary checkbox-inline align-checkboxes"
                                     title="Select all or none" data-bind="requiredAccess: 'DatabaseReadWrite'">
                                    <input type="checkbox" className="styled"
                                           data-bind="checkboxTriple: indexesSelectionState, event: { change: toggleSelectAll }"/>
                                    <label/>
                                </div>

                                <IndexFilter filter={filter} setFilter={setFilter} />
                                <IndexToolbarActions />
                            </div>
                        </div>
                        <IndexGlobalIndexing />
                    </div>
                )}
                <IndexFilterDescription filter={filter} groups={groups} />
                <button type="button" onClick={loadMissing}>Load Missing</button>
            </div>
            <div className="flex-grow scroll js-scroll-container">
                {groups.map(group => {
                    return (
                        <div key={group.name}>
                            <h2 className="on-base-background" title={"Collection: " + group.name}>
                                {group.name}
                            </h2>
                            {group.indexes.map(index =>
                                (
                                    <IndexPanel setPriority={p => setIndexPriority(index, p)}
                                                setLockMode={l => setIndexLockMode(index, l)}
                                                index={index}
                                                deleteIndex={() => promptDeleteIndexes(database, [index])}
                                                selected={selectedIndexes.includes(index.name)}
                                                toggleSelection={() => toggleSelection(index)}
                                                key={index.name}
                                    />
                                ))}
                        </div>
                    )
                })}
            </div>
        </div>
    );
}
