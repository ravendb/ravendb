import React, { useEffect, useState } from "react";
import { DatabasePanel } from "./partials/DatabasePanel";
import { useAccessManager } from "hooks/useAccessManager";
import { DatabasesSelectActions } from "./partials/DatabasesSelectActions";
import { DatabasesFilter } from "./partials/DatabasesFilter";
import { NoDatabases } from "./partials/NoDatabases";
import { Button, ButtonGroup, DropdownItem, DropdownMenu, DropdownToggle, UncontrolledDropdown } from "reactstrap";
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
import { Icon } from "components/common/Icon";

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

    const nodeTags = useAppSelector(clusterSelectors.allNodeTags);

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
                <div className="d-flex flex-wrap gap-3 align-items-center">
                    {canCreateNewDatabase && (
                        <UncontrolledDropdown>
                            <ButtonGroup className="rounded-group">
                                <Button color="primary" onClick={() => dispatch(openCreateDatabaseDialog())}>
                                    <Icon icon="database" addon="plus" />
                                    New database
                                </Button>
                                <DropdownToggle color="primary" caret></DropdownToggle>
                            </ButtonGroup>

                            <DropdownMenu>
                                <DropdownItem onClick={() => dispatch(openCreateDatabaseFromRestoreDialog())}>
                                    <i className="icon-restore-backup" /> New database from backup (Restore)
                                </DropdownItem>
                            </DropdownMenu>
                        </UncontrolledDropdown>
                    )}
                    <DatabasesFilter searchCriteria={filterCriteria} setFilterCriteria={setFilterCriteria} />
                </div>

                <DatabasesSelectActions
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
