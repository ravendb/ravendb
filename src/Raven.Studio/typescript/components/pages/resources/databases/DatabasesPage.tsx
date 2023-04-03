import React, { useEffect, useState } from "react";
import { DatabasePanel } from "./partials/DatabasePanel";
import { DatabasesToolbarActions } from "./partials/DatabasesToolbarActions";
import { DatabasesFilter } from "./partials/DatabasesFilter";
import { NoDatabases } from "./partials/NoDatabases";
import { DatabaseSharedInfo } from "../../../models/databases";
import { Row } from "reactstrap";
import { useAppDispatch, useAppSelector } from "components/store";
import {
    compactDatabase,
    loadDatabasesDetails,
    openCreateDatabaseFromRestoreDialog,
    selectAllDatabases,
    selectFilteredDatabases,
    syncDatabaseDetails,
} from "components/common/shell/databasesSlice";
import { useClusterTopologyManager } from "hooks/useClusterTopologyManager";
import router from "plugins/router";
import appUrl from "common/appUrl";

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

    // TODO: positioning create | select all | ...

    return (
        <div className="content-margin">
            <div id="dropdownContainer"></div> {/*fixes rendering order bug on hover animation */}
            <Row className="mb-4">
                <DatabasesToolbarActions
                    selectedDatabases={selectedDatabases}
                    filteredDatabases={filteredDatabases}
                    setSelectedDatabaseNames={(x) => setSelectedDatabaseNames(x)}
                />
            </Row>
            <DatabasesFilter />
            <div className="flex-grow scroll js-scroll-container">
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
