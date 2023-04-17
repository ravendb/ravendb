import React, { useEffect, useState } from "react";
import { DatabasePanel } from "./partials/DatabasePanel";
import { useAccessManager } from "hooks/useAccessManager";
import { DatabasesToolbarActions } from "./partials/DatabasesToolbarActions";
import { DatabasesFilter } from "./partials/DatabasesFilter";
import { NoDatabases } from "./partials/NoDatabases";
import { Button, Col, DropdownItem, DropdownMenu, DropdownToggle, Row, UncontrolledDropdown } from "reactstrap";
import { useAppDispatch, useAppSelector } from "components/store";
import router from "plugins/router";
import appUrl from "common/appUrl";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { shallowEqual } from "react-redux";
import { DatabaseFilterCriteria } from "components/models/databases";
import {
    compactDatabase,
    loadDatabasesDetails,
    openCreateDatabaseDialog,
    openCreateDatabaseFromRestoreDialog,
    syncDatabaseDetails,
} from "components/pages/resources/databases/store/databasesViewActions";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { databasesViewSelectors } from "components/pages/resources/databases/store/databasesViewSelectors";
import { StickyHeader } from "components/common/StickyHeader";

interface DatabasesPageProps {
    activeDatabase?: string;
    toggleSelectAll?: () => void;
}

interface DatabasesPageProps {
    compact?: string;
    restore?: boolean;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function DatabasesPage(props: DatabasesPageProps) {
    const databases = useAppSelector(databaseSelectors.allDatabases);

    const dispatch = useAppDispatch();

    const nodeTags = useAppSelector(clusterSelectors.clusterNodeTags);

    const [selectedDatabaseNames, setSelectedDatabaseNames] = useState<string[]>([]);

    const [filterCriteria, setFilterCriteria] = useState<DatabaseFilterCriteria>({
        name: "",
        states: [],
    });

    const { isOperatorOrAbove } = useAccessManager();
    const canCreateNewDatabase = isOperatorOrAbove();

    const filteredDatabaseNames = useAppSelector(
        databasesViewSelectors.filteredDatabaseNames(filterCriteria),
        shallowEqual
    );

    useEffect(() => {
        dispatch(loadDatabasesDetails(nodeTags));
    }, [dispatch, nodeTags]);

    useEffect(() => dispatch(syncDatabaseDetails()), [dispatch]);

    useEffect(() => {
        const visibleNames = filteredDatabaseNames;
        const nonVisibleSelection = selectedDatabaseNames.filter((x) => !visibleNames.includes(x));
        if (nonVisibleSelection.length) {
            setSelectedDatabaseNames((prev) => prev.filter((x) => !nonVisibleSelection.includes(x)));
        }
    }, [selectedDatabaseNames, filteredDatabaseNames]);

    const toggleSelection = (dbName: string) => {
        if (selectedDatabaseNames.includes(dbName)) {
            setSelectedDatabaseNames((s) => s.filter((x) => x !== dbName));
        } else {
            setSelectedDatabaseNames((s) => s.concat(dbName));
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
        <>
            <StickyHeader>
                <Row>
                    <Col sm="auto" className="align-self-center">
                        {canCreateNewDatabase && (
                            <UncontrolledDropdown group>
                                <Button color="primary" onClick={() => dispatch(openCreateDatabaseDialog())}>
                                    <i className="icon-new-database" />
                                    <span>New database</span>
                                </Button>
                                <DropdownToggle color="primary" caret></DropdownToggle>
                                <DropdownMenu>
                                    <DropdownItem onClick={() => dispatch(openCreateDatabaseFromRestoreDialog())}>
                                        <i className="icon-restore-backup" /> New database from backup (Restore)
                                    </DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        )}
                    </Col>
                    <Col>
                        <DatabasesFilter searchCriteria={filterCriteria} setFilterCriteria={setFilterCriteria} />
                    </Col>
                </Row>

                <DatabasesToolbarActions
                    databaseNames={filteredDatabaseNames}
                    selectedDatabases={selectedDatabases}
                    setSelectedDatabaseNames={setSelectedDatabaseNames}
                />
            </StickyHeader>
            <div id="dropdownContainer"></div> {/*fixes rendering order bug on hover animation */}
            <div className="p-4 pt-1">
                {filteredDatabaseNames.map((dbName) => (
                    <DatabasePanel
                        key={dbName}
                        databaseName={dbName}
                        selected={selectedDatabaseNames.includes(dbName)}
                        toggleSelection={() => toggleSelection(dbName)}
                    />
                ))}

                {!databases.length && <NoDatabases />}
            </div>
        </>
    );
}
