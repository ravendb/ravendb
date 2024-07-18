import React, { useEffect, useState } from "react";
import { DatabasePanel } from "./partials/DatabasePanel";
import { DatabasesSelectActions } from "./partials/DatabasesSelectActions";
import { DatabasesFilter } from "./partials/DatabasesFilter";
import { NoDatabases } from "./partials/NoDatabases";
import { Button } from "reactstrap";
import { useAppDispatch, useAppSelector } from "components/store";
import router from "plugins/router";
import appUrl from "common/appUrl";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { shallowEqual } from "react-redux";
import { DatabaseFilterCriteria } from "components/models/databases";
import {
    compactDatabase,
    loadDatabasesDetails,
    syncDatabaseDetails,
} from "components/pages/resources/databases/store/databasesViewActions";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { databasesViewSelectors } from "components/pages/resources/databases/store/databasesViewSelectors";
import { StickyHeader } from "components/common/StickyHeader";
import { Icon } from "components/common/Icon";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import CreateDatabase, { CreateDatabaseMode } from "./partials/create/CreateDatabase";

interface DatabasesPageProps {
    activeDatabase?: string;
    toggleSelectAll?: () => void;
}

interface DatabasesPageProps {
    compact?: string;
    restore?: boolean;
}

export function DatabasesPage(props: DatabasesPageProps) {
    const databases = useAppSelector(databaseSelectors.allDatabases);
    const nodeTags = useAppSelector(clusterSelectors.allNodeTags);
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);

    const dispatch = useAppDispatch();

    const [selectedDatabaseNames, setSelectedDatabaseNames] = useState<string[]>([]);

    const [filterCriteria, setFilterCriteria] = useState<DatabaseFilterCriteria>({
        name: "",
        states: [],
    });

    const [showFilterOptions, setShowFilterOptions] = useState(false);
    const [showToggleButton, setShowToggleButton] = useState(false);

    const toggleFilterOptions = () => {
        setShowFilterOptions(!showFilterOptions);
    };

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
        const handleResize = () => {
            const screenWidth = window.innerWidth;
            setShowFilterOptions(screenWidth >= 1400);
            setShowToggleButton(screenWidth < 1400);
        };

        handleResize();

        window.addEventListener("resize", handleResize);

        return () => {
            window.removeEventListener("resize", handleResize);
        };
    }, []);

    useEffect(() => {
        if (props.compact) {
            const toCompact = databases.find((x) => x.name === props.compact);
            if (toCompact) {
                dispatch(compactDatabase(toCompact));
            }
        }
        if (props.restore) {
            setCreateDatabaseMode("fromBackup");
        }

        // normalize url (strip extra params)
        router.navigate(appUrl.forDatabases(), {
            trigger: false,
            replace: true,
        });
    }, [props.compact, props.restore, databases, dispatch]);

    const selectedDatabases = databases.filter((x) => selectedDatabaseNames.includes(x.name));

    const [createDatabaseMode, setCreateDatabaseMode] = useState<CreateDatabaseMode>(null);

    return (
        <div className="content-margin">
            <StickyHeader>
                <div className="d-flex flex-wrap gap-3 align-items-end">
                    {isOperatorOrAbove && (
                        <>
                            <Button
                                color="primary"
                                onClick={() => setCreateDatabaseMode("regular")}
                                className="rounded-pill"
                            >
                                <Icon icon="database" addon="plus" />
                                New database
                            </Button>
                            {createDatabaseMode && (
                                <CreateDatabase
                                    closeModal={() => setCreateDatabaseMode(null)}
                                    initialMode={createDatabaseMode}
                                />
                            )}
                        </>
                    )}
                    {showToggleButton && (
                        <Button color="secondary" className="rounded-pill" onClick={toggleFilterOptions}>
                            <Icon icon="filter" />
                            {showFilterOptions ? "Hide Filtering Options" : "Show Filtering Options"}
                        </Button>
                    )}
                    <div className="d-flex flex-grow flex-wrap gap-3">
                        {showFilterOptions && (
                            <DatabasesFilter searchCriteria={filterCriteria} setFilterCriteria={setFilterCriteria} />
                        )}
                    </div>
                </div>

                <DatabasesSelectActions
                    databaseNames={filteredDatabaseNames}
                    selectedDatabases={selectedDatabases}
                    setSelectedDatabaseNames={setSelectedDatabaseNames}
                />
            </StickyHeader>
            <div id="dropdownContainer"></div> {/*fixes rendering order bug on hover animation */}
            <div className="mt-3">
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
    );
}
