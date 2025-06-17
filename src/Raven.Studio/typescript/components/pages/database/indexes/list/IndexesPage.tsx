import "./IndexesPage.scss";
import IndexFilter from "./IndexFilter";
import IndexSelectActions from "./IndexSelectActions";
import IndexUtils from "../../../../utils/IndexUtils";
import { useAppUrls } from "hooks/useAppUrls";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { LoadingView } from "components/common/LoadingView";
import { StickyHeader } from "components/common/StickyHeader";
import { BulkIndexOperationConfirm } from "components/pages/database/indexes/list/BulkIndexOperationConfirm";
import { ConfirmResetIndexes } from "components/pages/database/indexes/list/ConfirmResetIndexes";
import { useIndexesPage } from "components/pages/database/indexes/list/useIndexesPage";
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
import { ConditionalPopover } from "components/common/ConditionalPopover";
import Dropdown from "react-bootstrap/Dropdown";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import { CustomDropdownToggle } from "components/common/Dropdown";
import useBoolean from "components/hooks/useBoolean";
import { GlobalPauseIndexingConfirm } from "components/pages/resources/databases/partials/GlobalPauseIndexingConfirm";
import { GlobalResumeIndexingConfirm } from "components/pages/resources/databases/partials/GlobalResumeIndexingConfirm";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

interface IndexesPageProps {
    stale?: boolean;
    indexName?: string;
    isImportOpen?: boolean;
}

export function IndexesPage({ queryParams }: ReactQueryParamsProps<IndexesPageProps>) {
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
        allIndexes,
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
        asyncPauseGlobalIndexing,
        asyncResumeGlobalIndexing,
        globalStatusList,
    } = useIndexesPage(queryParams?.stale, queryParams?.isImportOpen);

    const deleteSelectedIndexes = () => {
        reportEvent("indexes", "delete-selected");
        return confirmDeleteIndexes(getSelectedIndexes());
    };

    const startSelectedIndexes = () => startIndexes(getSelectedIndexes());
    const disableSelectedIndexes = () => disableIndexes(getSelectedIndexes());
    const pauseSelectedIndexes = () => pauseIndexes(getSelectedIndexes());
    const resetSelectedIndexes = (mode?: Raven.Client.Documents.Indexes.IndexResetMode) => {
        return resetIndexData.openConfirm(
            allIndexes.filter((x) => selectedIndexes.includes(x.name)),
            mode
        );
    };

    const canPauseGlobalIndexing = globalStatusList?.some((x) => x.status === "Running");
    const canResumeGlobalIndexing = globalStatusList?.some((x) => x.status === "Paused");

    const { value: isGlobalPauseIndexingOpen, toggle: toggleIsGlobalPauseIndexingOpen } = useBoolean(false);
    const { value: isGlobalResumeIndexingOpen, toggle: toggleIsGlobalResumeIndexingOpen } = useBoolean(false);

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
        indexToHighlight: queryParams?.indexName,
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
                        <Col className="hstack gap-2">
                            {hasDatabaseWriteAccess && (
                                <ConditionalPopover
                                    conditions={{
                                        isActive: isNewIndexDisabled,
                                        message: (
                                            <div className="text-center">
                                                <Icon
                                                    icon={
                                                        staticClusterLimitStatus === "limitReached"
                                                            ? "cluster"
                                                            : "database"
                                                    }
                                                />
                                                {staticClusterLimitStatus === "limitReached" ? "Cluster" : "Database"}{" "}
                                                has reached the maximum number of static indexes allowed per{" "}
                                                {staticClusterLimitStatus === "limitReached" ? "cluster" : "database"}{" "}
                                                by your license.
                                                <br />
                                                Delete unused indexes or{" "}
                                                <strong>
                                                    <a href={upgradeLicenseLink} target="_blank">
                                                        upgrade your license
                                                    </a>
                                                </strong>
                                            </div>
                                        ),
                                    }}
                                >
                                    <Dropdown className="button-dropdown-pill" as={ButtonGroup}>
                                        <Button
                                            variant="primary"
                                            href={newIndexUrl}
                                            disabled={isNewIndexDisabled}
                                            className="button-dropdown-btn"
                                        >
                                            <Icon icon="index" addon="plus" />
                                            <span>New index</span>
                                        </Button>
                                        <Dropdown.Toggle
                                            variant="primary"
                                            className="dropdown-toggle button-dropdown-toggle"
                                            as={CustomDropdownToggle}
                                        />
                                        <Dropdown.Menu>
                                            <Dropdown.Item
                                                onClick={toggleIsImportIndexModalOpen}
                                                title="Import indexes from a file"
                                            >
                                                <Icon icon="index-import" />
                                                <span>Import indexes</span>
                                            </Dropdown.Item>
                                        </Dropdown.Menu>
                                    </Dropdown>
                                </ConditionalPopover>
                            )}
                            {canResumeGlobalIndexing && (
                                <>
                                    <ButtonWithSpinner
                                        variant="success"
                                        className="rounded-pill"
                                        onClick={toggleIsGlobalResumeIndexingOpen}
                                        isSpinning={asyncResumeGlobalIndexing.loading}
                                        icon="play"
                                    >
                                        Resume indexing
                                    </ButtonWithSpinner>
                                    {isGlobalResumeIndexingOpen && (
                                        <GlobalResumeIndexingConfirm
                                            toggle={toggleIsGlobalResumeIndexingOpen}
                                            onConfirm={asyncResumeGlobalIndexing.execute}
                                            allActionContexts={allActionContexts}
                                        />
                                    )}
                                </>
                            )}
                            {canPauseGlobalIndexing && (
                                <>
                                    <ButtonWithSpinner
                                        variant="secondary"
                                        className="rounded-pill"
                                        onClick={toggleIsGlobalPauseIndexingOpen}
                                        isSpinning={asyncPauseGlobalIndexing.loading}
                                        icon="pause"
                                    >
                                        Pause indexing
                                    </ButtonWithSpinner>
                                    {isGlobalPauseIndexingOpen && (
                                        <GlobalPauseIndexingConfirm
                                            toggle={toggleIsGlobalPauseIndexingOpen}
                                            onConfirm={asyncPauseGlobalIndexing.execute}
                                            allActionContexts={allActionContexts}
                                        />
                                    )}
                                </>
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
