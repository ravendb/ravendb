import React, { useCallback, useMemo, useState } from "react";
import { DatabasePanel } from "./DatabasePanel";
import { DatabasesToolbarActions } from "./DatabasesToolbarActions";
import { DatabasesFilter } from "./DatabasesFilter";
import { DatabasesCounter } from "./DatabasesCounter";
import { NoDatabases } from "./NoDatabases";
import { DatabaseFilterCriteria, DatabaseSharedInfo } from "../../../models/databases";
import { useChanges } from "hooks/useChanges";
import { Col, Row } from "reactstrap";
import { useAppSelector } from "components/store";
import { selectActiveDatabase, selectAllDatabases } from "components/common/shell/databasesSlice";

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

    const [filter, setFilter] = useState<DatabaseFilterCriteria>(() => ({
        searchText: "",
    }));

    const { serverNotifications } = useChanges();

    const [selectedDatabases, setSelectedDatabases] = useState<string[]>([]);

    const filteredDatabases = useMemo(() => {
        //TODO: filter and sort databases
        //TODO: update selection if needed
        return filterDatabases(databases, filter);
    }, [filter, databases]);

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
        if (databases && dbsCount === selectedCount) {
            return "checked";
        }

        if (selectedCount > 0) {
            return "some_checked";
        }

        return "unchecked";
    }, [filteredDatabases, selectedDatabases, databases]);

    const toggleSelection = (db: DatabaseSharedInfo) => {
        if (selectedDatabases.includes(db.name)) {
            setSelectedDatabases((s) => s.filter((x) => x !== db.name));
        } else {
            setSelectedDatabases((s) => s.concat(db.name));
        }
    };

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
                    <DatabasesToolbarActions />
                </Col>
            </Row>
            <div className="flex-grow scroll js-scroll-container">
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

                    {!databases.length && <NoDatabases />}
                </div>
            </div>
        </div>
    );
}
