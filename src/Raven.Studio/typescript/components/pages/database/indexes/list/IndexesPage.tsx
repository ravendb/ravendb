import React from "react";
import IndexFilter from "./IndexFilter";
import IndexSelectActions from "./IndexSelectActions";
import IndexUtils from "../../../../utils/IndexUtils";
import { useAppUrls } from "hooks/useAppUrls";
import "./IndexesPage.scss";
import {
    Button,
    Col,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    Row,
    UncontrolledDropdown,
    UncontrolledPopover,
} from "reactstrap";
import { LoadingView } from "components/common/LoadingView";
import { StickyHeader } from "components/common/StickyHeader";
import { BulkIndexOperationConfirm } from "components/pages/database/indexes/list/BulkIndexOperationConfirm";
import { ConfirmResetIndexes } from "components/pages/database/indexes/list/ConfirmResetIndexes";
import { getAllIndexes, useIndexesPage } from "components/pages/database/indexes/list/useIndexesPage";
import { useEventsCollector } from "hooks/useEventsCollector";
import { NoIndexes } from "components/pages/database/indexes/list/partials/NoIndexes";
import { Icon } from "components/common/Icon";
import { ConfirmSwapSideBySideIndex } from "./ConfirmSwapSideBySideIndex";
import ActionContextUtils from "components/utils/actionContextUtils";
import { getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import IndexesPageList, { IndexesPageListProps } from "./IndexesPageList";
import IndexesPageLicenseLimits from "./IndexesPageLicenseLimits";
import IndexesPageAboutView from "./IndexesPageAboutView";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { ImportIndexes } from "components/pages/database/indexes/list/migration/import/ImportIndexes";

interface IndexesPageProps {
    stale?: boolean;
    indexName?: string;
    isImportOpen?: boolean;
}

export function IndexesPage(props: IndexesPageProps) {
    const { stale, indexName: indexToHighlight, isImportOpen = false } = props;

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const { reportEvent } = useEventsCollector();

    const { forCurrentDatabase: urls } = useAppUrls();
    const newIndexUrl = urls.newIndex();

    const {
        loading,
        bulkOperationConfirm,
        setBulkOperationConfirm,
        resetIndexData,
        swapSideBySideData,
        stats,
        selectedIndexes,
        toggleSelectAll,
        onSelectCancel,
        filter,
        setFilter,
        filterByStatusOptions,
        filterByTypeOptions,
        regularIndexes,
        groups,
        replacements,
        highlightCallback,
        confirmSetLockModeSelectedIndexes,
        allIndexesCount,
        setIndexPriority,
        startIndexes,
        disableIndexes,
        pauseIndexes,
        setIndexLockMode,
        toggleSelection,
        openFaulty,
        getSelectedIndexes,
        confirmDeleteIndexes,
        globalIndexingStatus,
        isImportIndexModalOpen,
        toggleIsImportIndexModalOpen,
    } = useIndexesPage(stale, isImportOpen);

    const deleteSelectedIndexes = () => {
        reportEvent("indexes", "delete-selected");
        return confirmDeleteIndexes(getSelectedIndexes());
    };

    const startSelectedIndexes = () => startIndexes(getSelectedIndexes());
    const disableSelectedIndexes = () => disableIndexes(getSelectedIndexes());
    const pauseSelectedIndexes = () => pauseIndexes(getSelectedIndexes());
    const resetSelectedIndexes = (mode?: Raven.Client.Documents.Indexes.IndexResetMode) => {
        return resetIndexData.openConfirm(selectedIndexes, mode);
    };

    const allIndexes = getAllIndexes(groups, replacements);

    const allActionContexts = ActionContextUtils.getContexts(DatabaseUtils.getLocations(db));

    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    const autoClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfAutoIndexesPerCluster"));
    const staticClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfStaticIndexesPerCluster"));
    const autoDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfAutoIndexesPerDatabase"));
    const staticDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfStaticIndexesPerDatabase"));

    const autoClusterCount = useAppSelector(licenseSelectors.limitsUsage).NumberOfAutoIndexesInCluster;
    const staticClusterCount = useAppSelector(licenseSelectors.limitsUsage).NumberOfStaticIndexesInCluster;

    const autoDatabaseCount = stats.indexes.filter((x) => IndexUtils.isAutoIndex(x)).length;
    const staticDatabaseCount = stats.indexes.length - autoDatabaseCount;

    const autoClusterLimitStatus = getLicenseLimitReachStatus(autoClusterCount, autoClusterLimit);
    const staticClusterLimitStatus = getLicenseLimitReachStatus(staticClusterCount, staticClusterLimit);

    const autoDatabaseLimitStatus = getLicenseLimitReachStatus(autoDatabaseCount, autoDatabaseLimit);
    const staticDatabaseLimitStatus = getLicenseLimitReachStatus(staticDatabaseCount, staticDatabaseLimit);

    const isNewIndexDisabled =
        staticClusterLimitStatus === "limitReached" || staticDatabaseLimitStatus === "limitReached";

    if (loading) {
        return <LoadingView />;
    }

    if (stats.indexes.length === 0) {
        return (
            <>
                <NoIndexes />
                {isImportIndexModalOpen && <ImportIndexes toggle={toggleIsImportIndexModalOpen} />}
            </>
        );
    }

    const indexesPageListCommonProps: Omit<IndexesPageListProps, "indexes"> = {
        replacements,
        selectedIndexes,
        indexToHighlight,
        globalIndexingStatus,
        resetIndexData,
        swapSideBySideData,
        setIndexPriority,
        setIndexLockMode,
        openFaulty,
        startIndexes,
        disableIndexes,
        pauseIndexes,
        confirmDeleteIndexes,
        toggleSelection,
        highlightCallback,
    };

    return (
        <div className="content-margin">
            <IndexesPageLicenseLimits
                staticClusterLimitStatus={staticClusterLimitStatus}
                staticClusterCount={staticClusterCount}
                staticClusterLimit={staticClusterLimit}
                upgradeLicenseLink={upgradeLicenseLink}
                autoClusterLimitStatus={autoClusterLimitStatus}
                autoClusterCount={autoClusterCount}
                autoClusterLimit={autoClusterLimit}
                staticDatabaseLimitStatus={staticDatabaseLimitStatus}
                staticDatabaseCount={staticDatabaseCount}
                staticDatabaseLimit={staticDatabaseLimit}
                autoDatabaseLimitStatus={autoDatabaseLimitStatus}
                autoDatabaseCount={autoDatabaseCount}
                autoDatabaseLimit={autoDatabaseLimit}
            />

            {stats.indexes.length > 0 && (
                <StickyHeader>
                    <Row>
                        <Col className="hstack">
                            {hasDatabaseWriteAccess && (
                                <div id="NewIndexButton">
                                    <UncontrolledDropdown group className="button-dropdown-pill">
                                        <Button
                                            color="primary"
                                            href={newIndexUrl}
                                            disabled={isNewIndexDisabled}
                                            className="button-dropdown-btn"
                                        >
                                            <Icon icon="index" addon="plus" />
                                            <span>New index</span>
                                        </Button>
                                        <DropdownToggle
                                            className="dropdown-toggle button-dropdown-toggle"
                                            color="primary"
                                        />
                                        <DropdownMenu>
                                            <DropdownItem
                                                onClick={toggleIsImportIndexModalOpen}
                                                title="Import indexes from a file"
                                            >
                                                <Icon icon="index-import" />
                                                <span>Import indexes</span>
                                            </DropdownItem>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </div>
                            )}

                            {isNewIndexDisabled && (
                                <UncontrolledPopover
                                    trigger="hover"
                                    target="NewIndexButton"
                                    placement="top"
                                    className="bs5"
                                >
                                    <div className="p-3 text-center">
                                        <Icon
                                            icon={staticClusterLimitStatus === "limitReached" ? "cluster" : "database"}
                                        />
                                        {staticClusterLimitStatus === "limitReached" ? "Cluster" : "Database"} has
                                        reached the maximum number of static indexes allowed per{" "}
                                        {staticClusterLimitStatus === "limitReached" ? "cluster" : "database"} by your
                                        license.
                                        <br />
                                        Delete unused indexes or{" "}
                                        <strong>
                                            <a href={upgradeLicenseLink} target="_blank">
                                                upgrade your license
                                            </a>
                                        </strong>
                                    </div>
                                </UncontrolledPopover>
                            )}
                        </Col>
                        <Col xs="auto">
                            <IndexesPageAboutView
                                isUnlimited={
                                    staticClusterLimitStatus === "notReached" &&
                                    staticDatabaseLimitStatus === "notReached"
                                }
                            />
                        </Col>
                    </Row>
                    <IndexFilter
                        filter={filter}
                        setFilter={(x) => setFilter(x)}
                        filterByStatusOptions={filterByStatusOptions}
                        filterByTypeOptions={filterByTypeOptions}
                        indexesCount={allIndexesCount}
                    />

                    {/*  TODO  <IndexGlobalIndexing /> */}

                    {hasDatabaseWriteAccess && (
                        <IndexSelectActions
                            allIndexes={allIndexes}
                            selectedIndexes={selectedIndexes}
                            replacements={replacements}
                            deleteSelectedIndexes={deleteSelectedIndexes}
                            startSelectedIndexes={startSelectedIndexes}
                            disableSelectedIndexes={disableSelectedIndexes}
                            pauseSelectedIndexes={pauseSelectedIndexes}
                            resetSelectedIndexes={resetSelectedIndexes}
                            setLockModeSelectedIndexes={confirmSetLockModeSelectedIndexes}
                            toggleSelectAll={toggleSelectAll}
                            onCancel={onSelectCancel}
                        />
                    )}
                </StickyHeader>
            )}
            <div className="indexes mt-3 pt-0 no-transition">
                <div className="indexes-list">
                    {filter.groupBy === "None" && (
                        <IndexesPageList {...indexesPageListCommonProps} indexes={regularIndexes} />
                    )}
                    {filter.groupBy === "Collection" &&
                        groups.map((group) => {
                            return (
                                <div className="mb-4" key={"group-" + group.name}>
                                    <h2 className="mt-0" title={"Collection: " + group.name}>
                                        {group.name}
                                    </h2>
                                    <IndexesPageList {...indexesPageListCommonProps} indexes={group.indexes} />
                                </div>
                            );
                        })}
                </div>
            </div>

            {bulkOperationConfirm && (
                <BulkIndexOperationConfirm {...bulkOperationConfirm} toggle={() => setBulkOperationConfirm(null)} />
            )}
            {resetIndexData.confirmData && (
                <ConfirmResetIndexes
                    {...resetIndexData.confirmData}
                    closeConfirm={resetIndexData.closeConfirm}
                    onConfirm={resetIndexData.onConfirm}
                    allActionContexts={allActionContexts}
                />
            )}
            {swapSideBySideData.indexName && (
                <ConfirmSwapSideBySideIndex
                    indexName={swapSideBySideData.indexName}
                    toggle={() => swapSideBySideData.setIndexName(null)}
                    onConfirm={(x) => swapSideBySideData.onConfirm(x)}
                    allActionContexts={allActionContexts}
                />
            )}
            {isImportIndexModalOpen && <ImportIndexes toggle={toggleIsImportIndexModalOpen} />}
        </div>
    );
}
