import React, { useEffect, useState } from "react";
import { DatabasePanel } from "./partials/DatabasePanel";
import { DatabasesToolbarActions } from "./partials/DatabasesToolbarActions";
import { DatabasesFilter } from "./partials/DatabasesFilter";
import { NoDatabases } from "./partials/NoDatabases";
import { Row } from "reactstrap";
import { useAppDispatch, useAppSelector } from "components/store";
import router from "plugins/router";
import appUrl from "common/appUrl";
import { selectClusterNodeTags } from "components/common/shell/clusterSlice";
import { shallowEqual } from "react-redux";
import { selectAllDatabases, selectFilteredDatabaseNames } from "components/common/shell/databaseSliceSelectors";
import {
    compactDatabase,
    loadDatabasesDetails,
    openCreateDatabaseFromRestoreDialog,
    syncDatabaseDetails,
} from "components/common/shell/databaseSliceActions";

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
    const databases = useAppSelector(selectAllDatabases);

    const dispatch = useAppDispatch();

    const nodeTags = useAppSelector(selectClusterNodeTags);

    const [selectedDatabaseNames, setSelectedDatabaseNames] = useState<string[]>([]);

    const filteredDatabaseNames = useAppSelector(selectFilteredDatabaseNames, shallowEqual);

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
        <div className="content-margin">
            <div id="dropdownContainer"></div> {/*fixes rendering order bug on hover animation */}
            <Row className="mb-4">
                <DatabasesToolbarActions
                    databaseNames={filteredDatabaseNames}
                    selectedDatabases={selectedDatabases}
                    setSelectedDatabaseNames={setSelectedDatabaseNames}
                />
            </Row>
            <DatabasesFilter />
            <div className="flex-grow scroll js-scroll-container">
                <div>
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
            </div>
        </div>
    );
}
