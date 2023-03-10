import React, { useCallback, useEffect, useMemo, useState } from "react";
import { DatabasePanel } from "./partials/DatabasePanel";
import { DatabasesToolbarActions } from "./partials/DatabasesToolbarActions";
import { DatabasesFilter } from "./partials/DatabasesFilter";
import { DatabasesCounter } from "./partials/DatabasesCounter";
import { NoDatabases } from "./partials/NoDatabases";
import { DatabaseFilterCriteria, DatabaseSharedInfo } from "../../../models/databases";
import { Col, Row } from "reactstrap";
import { useAppDispatch, useAppSelector } from "components/store";
import {
    compactDatabase,
    loadDatabaseDetails,
    openCreateDatabaseFromRestoreDialog,
    selectAllDatabases,
} from "components/common/shell/databasesSlice";
import { useClusterTopologyManager } from "hooks/useClusterTopologyManager";
import router from "plugins/router";
import appUrl from "common/appUrl";

interface DatabasesPageProps {
    activeDatabase?: string;
}

function filterDatabases(databases: DatabaseSharedInfo[], criteria: DatabaseFilterCriteria) {
    if (criteria.searchText) {
        return databases.filter((x) => x.name.toLowerCase().includes(criteria.searchText.toLowerCase()));
    }

    return databases;
}

interface DatabasesPageProps {
    compact?: string;
    restore?: boolean;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function DatabasesPage(props: DatabasesPageProps) {
    const databases = useAppSelector(selectAllDatabases);

    const dispatch = useAppDispatch();

    const [filter, setFilter] = useState<DatabaseFilterCriteria>(() => ({
        searchText: "",
    }));

    const { nodeTags } = useClusterTopologyManager();

    const [selectedDatabaseNames, setSelectedDatabaseNames] = useState<string[]>([]);

    const filteredDatabases = useMemo(() => {
        //TODO: filter and sort databases
        //TODO: update selection if needed
        return filterDatabases(databases, filter);
    }, [filter, databases]);

    useEffect(() => {
        //TODO: make sure we can reload details when we add/remove db
        dispatch(loadDatabaseDetails(nodeTags));
    }, [dispatch, nodeTags]);

    useEffect(() => {
        const visibleNames = filteredDatabases.map((x) => x.name);
        const nonVisibleSelection = selectedDatabaseNames.filter((x) => !visibleNames.includes(x));
        if (nonVisibleSelection.length) {
            setSelectedDatabaseNames((prev) => prev.filter((x) => !nonVisibleSelection.includes(x)));
        }
    }, [selectedDatabaseNames, filteredDatabases]);

    const toggleSelectAll = useCallback(() => {
        const selectedCount = selectedDatabaseNames.length;

        if (selectedCount > 0) {
            setSelectedDatabaseNames([]);
        } else {
            setSelectedDatabaseNames(filteredDatabases.map((x) => x.name));
        }
    }, [selectedDatabaseNames, filteredDatabases]);

    const databasesSelectionState = useMemo<checkbox>(() => {
        const selectedCount = selectedDatabaseNames.length;
        const dbsCount = filteredDatabases.length;
        if (dbsCount > 0 && dbsCount === selectedCount) {
            return "checked";
        }

        if (selectedCount > 0) {
            return "some_checked";
        }

        return "unchecked";
    }, [filteredDatabases, selectedDatabaseNames]);

    const toggleSelection = (db: DatabaseSharedInfo) => {
        if (selectedDatabaseNames.includes(db.name)) {
            setSelectedDatabaseNames((s) => s.filter((x) => x !== db.name));
        } else {
            setSelectedDatabaseNames((s) => s.concat(db.name));
        }
    };

    useEffect(() => {
        if (props.compact) {
            const toCompact = databases.find((x) => x.name === props.compact);
            if (toCompact) {
                dispatch(compactDatabase(toCompact));
            }
        }
        if (props.restore) {
            dispatch(openCreateDatabaseFromRestoreDialog());
        }

        // normalize url (strip extra params)
        router.navigate(appUrl.forDatabases(), {
            trigger: false,
            replace: true,
        });
    }, [props.compact, props.restore, databases, dispatch]);

    const selectedDatabases = databases.filter((x) => selectedDatabaseNames.includes(x.name));

    return (
        <div className="content-margin">
            <Row className="mb-4">
                <Col sm="auto">
                    <DatabasesFilter
                        filter={filter}
                        setFilter={setFilter}
                        selectionState={databasesSelectionState}
                        toggleSelectAll={toggleSelectAll}
                    />
                </Col>
                <Col>
                    <DatabasesToolbarActions selectedDatabases={selectedDatabases} />
                </Col>
            </Row>
            <div className="flex-grow scroll js-scroll-container">
                <DatabasesCounter />
                <div>
                    {filteredDatabases.map((db) => (
                        <DatabasePanel
                            key={db.name}
                            selected={selectedDatabaseNames.includes(db.name)}
                            toggleSelection={() => toggleSelection(db)}
                            db={db}
                        />
                    ))}

                    {!databases.length && <NoDatabases />}
                </div>
            </div>
        </div>
    );
}
