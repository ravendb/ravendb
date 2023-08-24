import React from "react";
import database from "models/resources/database";
import { IndexPanel } from "./IndexPanel";
import IndexFilter from "./IndexFilter";
import IndexSelectActions from "./IndexSelectActions";
import IndexUtils from "../../../../utils/IndexUtils";
import { useAccessManager } from "hooks/useAccessManager";
import { useAppUrls } from "hooks/useAppUrls";
import "./IndexesPage.scss";
import { Alert, Button, Card, Col, Row } from "reactstrap";
import { LoadingView } from "components/common/LoadingView";
import { StickyHeader } from "components/common/StickyHeader";
import { BulkIndexOperationConfirm } from "components/pages/database/indexes/list/BulkIndexOperationConfirm";
import { ConfirmResetIndex } from "components/pages/database/indexes/list/ConfirmResetIndex";
import { getAllIndexes, useIndexesPage } from "components/pages/database/indexes/list/useIndexesPage";
import { useEventsCollector } from "hooks/useEventsCollector";
import { NoIndexes } from "components/pages/database/indexes/list/partials/NoIndexes";
import { Icon } from "components/common/Icon";
import { ConfirmSwapSideBySideIndex } from "./ConfirmSwapSideBySideIndex";
import ActionContextUtils from "components/utils/actionContextUtils";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import AboutViewFloating, { AccordionItemLicensing, AccordionItemWrapper } from "components/common/AboutView";
import { LicenseLimitThreshold } from "components/common/LicenseLimitThreshold";

interface IndexesPageProps {
    db: database;
    stale?: boolean;
    indexName?: string;
}

export function IndexesPage(props: IndexesPageProps) {
    const { db, stale, indexName: indexToHighlight } = props;

    const staticIndexServerLimit = 12 * 5; //TODO
    const autoIndexServerLimit = 24 * 5; //TODO
    const staticIndexServerCount = 12 * 5; //TODO
    const autoIndexServerCount = 24 * 5; //TODO

    const { canReadWriteDatabase } = useAccessManager();
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
    } = useIndexesPage(db, stale);

    const deleteSelectedIndexes = () => {
        reportEvent("indexes", "delete-selected");
        return confirmDeleteIndexes(db, getSelectedIndexes());
    };

    const startSelectedIndexes = () => startIndexes(getSelectedIndexes());
    const disableSelectedIndexes = () => disableIndexes(getSelectedIndexes());
    const pauseSelectedIndexes = () => pauseIndexes(getSelectedIndexes());

    const indexNames = getAllIndexes(groups, replacements).map((x) => x.name);

    const allActionContexts = ActionContextUtils.getContexts(db.getLocations());

    if (loading) {
        return <LoadingView />;
    }

    if (stats.indexes.length === 0) {
        return <NoIndexes database={db} />;
    }

    return (
        <>
            <LicenseLimitThreshold count={staticIndexServerCount} limit={staticIndexServerLimit}>
                <Alert color="warning" className="text-center">
                    Your server is reaching the <strong>maximum number of static indexes</strong> allowed by your
                    license{" "}
                    <strong>
                        ({staticIndexServerCount}/{staticIndexServerLimit})
                    </strong>
                    <br /> Delete unused indexes or{" "}
                    <strong>
                        <a href="https://ravendb.net/buy" target="_blank">
                            upgrade your license
                        </a>
                    </strong>
                </Alert>
            </LicenseLimitThreshold>
            <LicenseLimitThreshold count={autoIndexServerCount} limit={autoIndexServerLimit}>
                <Alert color="warning" className="text-center">
                    Your server is reaching the <strong>maximum number of auto indexes</strong> allowed by your license{" "}
                    <strong>
                        ({autoIndexServerCount}/{autoIndexServerLimit})
                    </strong>
                    <br /> Delete unused indexes or{" "}
                    <strong>
                        <a href="https://ravendb.net/buy" target="_blank">
                            upgrade your license
                        </a>
                    </strong>
                </Alert>
            </LicenseLimitThreshold>
            {stats.indexes.length > 0 && (
                <StickyHeader>
                    <Row>
                        <Col>
                            <Button color="primary" href={newIndexUrl} className="rounded-pill px-3">
                                <Icon icon="index" addon="plus" />
                                <span>New index</span>
                            </Button>
                        </Col>
                        <Col xs="auto">
                            <AboutViewFloating>
                                <AccordionItemWrapper
                                    icon="index"
                                    color="info"
                                    heading="About this view"
                                    description="Get additional info on what this feature can offer you"
                                    targetId="1"
                                >
                                    <p>
                                        <strong>Admin JS Console</strong> is a specialized feature primarily intended
                                        for resolving server errors. It provides a direct interface to the underlying
                                        system, granting the capacity to execute scripts for intricate server
                                        operations.
                                    </p>
                                    <p>
                                        It is predominantly intended for advanced troubleshooting and rectification
                                        procedures executed by system administrators or RavenDB support.
                                    </p>
                                    <hr />
                                    <div className="small-label mb-2">useful links</div>
                                    <a href="https://ravendb.net/l/IBUJ7M/6.0/Csharp" target="_blank">
                                        <Icon icon="newtab" /> Docs - Admin JS Console
                                    </a>
                                </AccordionItemWrapper>
                                <AccordionItemWrapper
                                    icon="road-cone"
                                    color="success"
                                    heading="Examples of use"
                                    description="Learn how to get the most of this feature"
                                    targetId="2"
                                >
                                    <p>
                                        <strong>To set the refresh time:</strong> enter the appropriate date in the
                                        metadata <code>@refresh</code> property.
                                    </p>
                                    <p>
                                        <strong>Note:</strong> RavenDB scans which documents should be refreshed at the
                                        frequency specified. The actual refresh time can increase (up to) that value.
                                    </p>
                                </AccordionItemWrapper>
                                <AccordionItemWrapper
                                    icon="license"
                                    color="warning"
                                    heading="Licensing"
                                    description="See which plans offer this and more exciting features"
                                    targetId="3"
                                    pill
                                    pillText="Upgrade available"
                                    pillIcon="star-filled"
                                >
                                    <AccordionItemLicensing
                                        description="This feature is not available in your license. Unleash the full potential and upgrade your plan."
                                        featureName="Document Compression"
                                        featureIcon="documents-compression"
                                        checkedLicenses={["Professional", "Enterprise"]}
                                    >
                                        <p className="lead fs-4">Get your license expanded</p>
                                        <div className="mb-3">
                                            <Button color="primary" className="rounded-pill">
                                                <Icon icon="notifications" />
                                                Contact us
                                            </Button>
                                        </div>
                                        <small>
                                            <a href="#" target="_blank" className="text-muted">
                                                See pricing plans
                                            </a>
                                        </small>
                                    </AccordionItemLicensing>
                                </AccordionItemWrapper>
                            </AboutViewFloating>
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

                    {canReadWriteDatabase(db) && (
                        <IndexSelectActions
                            indexNames={indexNames}
                            selectedIndexes={selectedIndexes}
                            deleteSelectedIndexes={deleteSelectedIndexes}
                            startSelectedIndexes={startSelectedIndexes}
                            disableSelectedIndexes={disableSelectedIndexes}
                            pauseSelectedIndexes={pauseSelectedIndexes}
                            setLockModeSelectedIndexes={confirmSetLockModeSelectedIndexes}
                            toggleSelectAll={toggleSelectAll}
                            onCancel={onSelectCancel}
                        />
                    )}
                </StickyHeader>
            )}
            <div className="indexes p-4 pt-0 no-transition">
                <div className="indexes-list">
                    {groups.map((group) => {
                        return (
                            <div className="mb-4" key={"group-" + group.name}>
                                <h2 className="mt-0" title={"Collection: " + group.name}>
                                    {group.name}
                                </h2>

                                {group.indexes.map((index) => {
                                    const replacement = replacements.find(
                                        (x) => x.name === IndexUtils.SideBySideIndexPrefix + index.name
                                    );
                                    return (
                                        <React.Fragment key={index.name}>
                                            <IndexPanel
                                                setPriority={(p) => setIndexPriority(index, p)}
                                                setLockMode={(l) => setIndexLockMode(index, l)}
                                                globalIndexingStatus={globalIndexingStatus}
                                                resetIndex={() => resetIndexData.setIndexName(index.name)}
                                                openFaulty={(location: databaseLocationSpecifier) =>
                                                    openFaulty(index, location)
                                                }
                                                startIndexing={() => startIndexes([index])}
                                                disableIndexing={() => disableIndexes([index])}
                                                pauseIndexing={() => pauseIndexes([index])}
                                                index={index}
                                                hasReplacement={!!replacement}
                                                database={db}
                                                deleteIndex={() => confirmDeleteIndexes(db, [index])}
                                                selected={selectedIndexes.includes(index.name)}
                                                toggleSelection={() => toggleSelection(index)}
                                                key={index.name}
                                                ref={indexToHighlight === index.name ? highlightCallback : undefined}
                                            />
                                            {replacement && (
                                                <Card className="mb-0 px-5 py-2 bg-faded-warning">
                                                    <div className="flex-horizontal">
                                                        <div className="title me-4">
                                                            <Icon icon="swap" /> Side by side
                                                        </div>
                                                        <ButtonWithSpinner
                                                            color="warning"
                                                            size="sm"
                                                            onClick={() => swapSideBySideData.setIndexName(index.name)}
                                                            title="Click to replace the current index definition with the replacement index"
                                                            isSpinning={swapSideBySideData.inProgress(index.name)}
                                                            icon="force"
                                                        >
                                                            Swap now
                                                        </ButtonWithSpinner>
                                                    </div>
                                                </Card>
                                            )}
                                            {replacement && (
                                                <IndexPanel
                                                    setPriority={(p) => setIndexPriority(replacement, p)}
                                                    setLockMode={(l) => setIndexLockMode(replacement, l)}
                                                    globalIndexingStatus={globalIndexingStatus}
                                                    resetIndex={() => resetIndexData.setIndexName(replacement.name)}
                                                    openFaulty={(location: databaseLocationSpecifier) =>
                                                        openFaulty(replacement, location)
                                                    }
                                                    startIndexing={() => startIndexes([replacement])}
                                                    disableIndexing={() => disableIndexes([replacement])}
                                                    pauseIndexing={() => pauseIndexes([replacement])}
                                                    index={replacement}
                                                    database={db}
                                                    deleteIndex={() => confirmDeleteIndexes(db, [replacement])}
                                                    selected={selectedIndexes.includes(replacement.name)}
                                                    toggleSelection={() => toggleSelection(replacement)}
                                                    key={replacement.name}
                                                    ref={undefined}
                                                />
                                            )}
                                        </React.Fragment>
                                    );
                                })}
                            </div>
                        );
                    })}
                </div>
            </div>

            {bulkOperationConfirm && (
                <BulkIndexOperationConfirm {...bulkOperationConfirm} toggle={() => setBulkOperationConfirm(null)} />
            )}
            {resetIndexData.indexName && (
                <ConfirmResetIndex
                    indexName={resetIndexData.indexName}
                    toggle={() => resetIndexData.setIndexName(null)}
                    onConfirm={(x) => resetIndexData.onConfirm(x)}
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
        </>
    );
}
