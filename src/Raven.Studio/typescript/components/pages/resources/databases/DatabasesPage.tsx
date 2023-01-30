import React, { useCallback, useMemo, useState } from "react";
import { DatabasePanel } from "./DatabasePanel";
import { DatabasesToolbarActions } from "./DatabasesToolbarActions";
import { DatabasesFilter } from "./DatabasesFilter";
import { DatabasesCounter } from "./DatabasesCounter";
import { NoDatabases } from "./NoDatabases";
import { DatabaseFilterCriteria, DatabaseSharedInfo } from "../../../models/databases";
import { useChanges } from "hooks/useChanges";
import { Col, Row } from "reactstrap";
import { useAppDispatch, useAppSelector } from "components/store";
import {
    openDeleteDatabasesDialog,
    selectActiveDatabase,
    selectAllDatabases,
} from "components/common/shell/databasesSlice";
import { dispatch } from "d3";

interface DatabasesPageProps {
    activeDatabase?: string;
}

function filterDatabases(databases: DatabaseSharedInfo[], criteria: DatabaseFilterCriteria) {
    if (criteria.searchText) {
        return databases.filter((x) => x.name.toLowerCase().includes(criteria.searchText.toLowerCase()));
    }

    return databases;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function DatabasesPage() {
    //TODO: highlight active database

    const activeDatabase = useAppSelector(selectActiveDatabase);
    const databases = useAppSelector(selectAllDatabases);

    const dispatch = useAppDispatch();

    const [filter, setFilter] = useState<DatabaseFilterCriteria>(() => ({
        searchText: "",
    }));

    const { serverNotifications } = useChanges();

    const [selectedDatabaseNames, setSelectedDatabaseNames] = useState<string[]>([]);

    const filteredDatabases = useMemo(() => {
        //TODO: filter and sort databases
        //TODO: update selection if needed
        return filterDatabases(databases, filter);
    }, [filter, databases]);

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
        if (databases && dbsCount === selectedCount) {
            return "checked";
        }

        if (selectedCount > 0) {
            return "some_checked";
        }

        return "unchecked";
    }, [filteredDatabases, selectedDatabaseNames, databases]);

    const toggleSelection = (db: DatabaseSharedInfo) => {
        if (selectedDatabaseNames.includes(db.name)) {
            setSelectedDatabaseNames((s) => s.filter((x) => x !== db.name));
        } else {
            setSelectedDatabaseNames((s) => s.concat(db.name));
        }
    };

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
