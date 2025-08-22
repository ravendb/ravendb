import { useEffect, useState } from "react";
import { DatabasesSelectActions } from "./partials/DatabasesSelectActions";
import { DatabasesFilter } from "./partials/DatabasesFilter";
import { NoDatabases } from "./partials/NoDatabases";
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
import { Icon } from "components/common/Icon";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import CreateDatabase, { CreateDatabaseMode } from "./partials/create/CreateDatabase";
import Button from "react-bootstrap/Button";
import SizeGetter from "components/common/SizeGetter";
import DatabasesList from "./partials/DatabasesList";

interface DatabasesPageProps {
    compact?: string;
    shard?: number;
    restore?: boolean;
}

export function DatabasesPage({ queryParams }: ReactQueryParamsProps<DatabasesPageProps>) {
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
        if (queryParams?.compact) {
            const toCompact = databases.find((x) => x.name === queryParams?.compact);
            if (toCompact) {
                dispatch(compactDatabase(toCompact, queryParams?.shard));
            }
        }
        if (queryParams?.restore) {
            setCreateDatabaseMode("fromBackup");
        }

        // normalize url (strip extra params)
        router.navigate(appUrl.forDatabases(), {
            trigger: false,
            replace: true,
        });
    }, [queryParams?.compact, queryParams?.restore, databases, dispatch, queryParams?.shard]);

    const selectedDatabases = databases.filter((x) => selectedDatabaseNames.includes(x.name));

    const [createDatabaseMode, setCreateDatabaseMode] = useState<CreateDatabaseMode>(null);

    return (
        <div className="h-100 vstack">
            <div className="d-flex flex-wrap gap-3 align-items-end px-4 pt-4">
                {isOperatorOrAbove && (
                    <>
                        <Button
                            variant="primary"
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
                    <Button variant="secondary" className="rounded-pill" onClick={toggleFilterOptions}>
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
            {databases.length > 0 ? (
                <div className="flex-grow-1">
                    <SizeGetter
                        isHeighRequired
                        render={({ height }) => (
                            <DatabasesList
                                maxHeight={height}
                                filteredDatabaseNames={filteredDatabaseNames}
                                selectedDatabaseNames={selectedDatabaseNames}
                                toggleSelection={toggleSelection}
                            />
                        )}
                    />
                </div>
            ) : (
                <NoDatabases />
            )}
        </div>
    );
}
