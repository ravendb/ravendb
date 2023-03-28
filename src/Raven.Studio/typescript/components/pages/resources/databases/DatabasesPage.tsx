import React, { useCallback, useEffect, useMemo, useState } from "react";
import { DatabasePanel } from "./partials/DatabasePanel";
import { DatabasesToolbarActions } from "./partials/DatabasesToolbarActions";
import { DatabasesFilter } from "./partials/DatabasesFilter";
import { DatabasesCounter } from "./partials/DatabasesCounter";
import { NoDatabases } from "./partials/NoDatabases";
import { DatabaseSharedInfo } from "../../../models/databases";
import { Col, Row } from "reactstrap";
import { useAppDispatch, useAppSelector } from "components/store";
import {
    compactDatabase,
    loadDatabasesDetails,
    openCreateDatabaseFromRestoreDialog,
    selectAllDatabases,
    selectDatabaseSearchCriteria,
    selectFilteredDatabases,
    syncDatabaseDetails,
} from "components/common/shell/databasesSlice";
import { useClusterTopologyManager } from "hooks/useClusterTopologyManager";
import router from "plugins/router";
import appUrl from "common/appUrl";
import { shallowEqual } from "react-redux";

interface DatabasesPageProps {
    activeDatabase?: string;
}

interface DatabasesPageProps {
    compact?: string;
    restore?: boolean;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function DatabasesPage(props: DatabasesPageProps) {
    const databases = useAppSelector(selectAllDatabases);
    const searchCriteria = useAppSelector(selectDatabaseSearchCriteria, shallowEqual);

    const dispatch = useAppDispatch();

    const { nodeTags } = useClusterTopologyManager();

    const [selectedDatabaseNames, setSelectedDatabaseNames] = useState<string[]>([]);

    const filteredDatabases = useAppSelector(selectFilteredDatabases);

    useEffect(() => {
        dispatch(loadDatabasesDetails(nodeTags));
    }, [dispatch, nodeTags]);

    useEffect(() => {
        return dispatch(syncDatabaseDetails());
    }, [dispatch]);

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
            <div id="dropdownContainer"></div> {/*fixes rendering order bug on hover animation */}
            <Row className="mb-4">
                <Col sm="auto">
                    <DatabasesFilter
                        filter={searchCriteria}
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
