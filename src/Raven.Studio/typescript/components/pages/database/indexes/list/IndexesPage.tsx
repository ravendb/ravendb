import React from "react";
import database from "models/resources/database";
import { IndexPanel } from "./IndexPanel";
import IndexFilter, { IndexFilterDescription } from "./IndexFilter";
import IndexToolbarActions from "./IndexToolbarActions";
import IndexUtils from "../../../../utils/IndexUtils";
import { CheckboxTriple } from "components/common/CheckboxTriple";
import { useAccessManager } from "hooks/useAccessManager";

import "./IndexesPage.scss";
import { Button, Card, Col, Row, Spinner } from "reactstrap";
import { LoadingView } from "components/common/LoadingView";
import { StickyHeader } from "components/common/StickyHeader";
import { BulkIndexOperationConfirm } from "components/pages/database/indexes/list/BulkIndexOperationConfirm";
import { ConfirmResetIndex } from "components/pages/database/indexes/list/ConfirmResetIndex";
import { getAllIndexes, useIndexesPage } from "components/pages/database/indexes/list/useIndexesPage";
import { useEventsCollector } from "hooks/useEventsCollector";
import { NoIndexes } from "components/pages/database/indexes/list/partials/NoIndexes";

interface IndexesPageProps {
    database: database;
    stale?: boolean;
    indexName?: string;
}

export function IndexesPage(props: IndexesPageProps) {
    const { database, stale, indexName: indexToHighlight } = props;

    const { canReadWriteDatabase } = useAccessManager();
    const { reportEvent } = useEventsCollector();

    const {
        loading,
        bulkOperationConfirm,
        setBulkOperationConfirm,
        resetIndexConfirm,
        setResetIndexConfirm,
        stats,
        selectedIndexes,
        toggleSelectAll,
        filter,
        setFilter,
        groups,
        replacements,
        swapNowProgress,
        highlightCallback,
        confirmSwapSideBySide,
        confirmSetLockModeSelectedIndexes,
        indexesSelectionState,
        setIndexPriority,
        toggleDisableIndexes,
        togglePauseIndexes,
        setIndexLockMode,
        resetIndex,
        toggleSelection,
        onResetIndexConfirm,
        openFaulty,
        getSelectedIndexes,
        confirmDeleteIndexes,
        globalIndexingStatus,
    } = useIndexesPage(database, stale);

    const deleteSelectedIndexes = () => {
        reportEvent("indexes", "delete-selected");
        return confirmDeleteIndexes(database, getSelectedIndexes());
    };

    const enableSelectedIndexes = () => toggleDisableIndexes(true, getSelectedIndexes());
    const disableSelectedIndexes = () => toggleDisableIndexes(false, getSelectedIndexes());
    const resumeSelectedIndexes = () => togglePauseIndexes(true, getSelectedIndexes());
    const pauseSelectedIndexes = () => togglePauseIndexes(false, getSelectedIndexes());

    if (loading) {
        return <LoadingView />;
    }

    if (stats.indexes.length === 0) {
        return <NoIndexes database={database} />;
    }

    return (
        <>
            <StickyHeader>
                {stats.indexes.length > 0 && (
                    <Row>
                        <Col>
                            <Row>
                                <Col sm="auto">
                                    {canReadWriteDatabase(database) && (
                                        <CheckboxTriple
                                            onChanged={toggleSelectAll}
                                            state={indexesSelectionState()}
                                            title="Select all or none"
                                        />
                                    )}
                                </Col>
                                <Col>
                                    <IndexFilter filter={filter} setFilter={setFilter} />
                                </Col>
                            </Row>
                        </Col>
                        <Col sm="auto">
                            {canReadWriteDatabase(database) && (
                                <IndexToolbarActions
                                    selectedIndexes={selectedIndexes}
                                    deleteSelectedIndexes={deleteSelectedIndexes}
                                    enableSelectedIndexes={enableSelectedIndexes}
                                    disableSelectedIndexes={disableSelectedIndexes}
                                    pauseSelectedIndexes={pauseSelectedIndexes}
                                    resumeSelectedIndexes={resumeSelectedIndexes}
                                    setLockModeSelectedIndexes={confirmSetLockModeSelectedIndexes}
                                />
                            )}
                        </Col>
                        {/*  TODO  <IndexGlobalIndexing /> */}
                    </Row>
                )}
                <IndexFilterDescription filter={filter} indexes={getAllIndexes(groups, replacements)} />
            </StickyHeader>
            <div className="indexes content-margin no-transition">
                <div className="indexes-list">
                    {groups.map((group) => {
                        return (
                            <div key={"group-" + group.name}>
                                <h2 className="on-base-background mt-4" title={"Collection: " + group.name}>
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
                                                resetIndex={() => resetIndex(index)}
                                                openFaulty={(location: databaseLocationSpecifier) =>
                                                    openFaulty(index, location)
                                                }
                                                enableIndexing={() => toggleDisableIndexes(true, [index])}
                                                disableIndexing={() => toggleDisableIndexes(false, [index])}
                                                pauseIndexing={() => togglePauseIndexes(false, [index])}
                                                resumeIndexing={() => togglePauseIndexes(true, [index])}
                                                index={index}
                                                hasReplacement={!!replacement}
                                                database={database}
                                                deleteIndex={() => confirmDeleteIndexes(database, [index])}
                                                selected={selectedIndexes.includes(index.name)}
                                                toggleSelection={() => toggleSelection(index)}
                                                key={index.name}
                                                ref={indexToHighlight === index.name ? highlightCallback : undefined}
                                            />
                                            {replacement && (
                                                <Card className="sidebyside-actions px-5 py-2 bg-faded-warning">
                                                    <div className="flex-horizontal">
                                                        <div className="title me-4">
                                                            <i className="icon-swap" /> Side by side
                                                        </div>
                                                        <Button
                                                            color="warning"
                                                            size="sm"
                                                            disabled={swapNowProgress.includes(index.name)}
                                                            onClick={() => confirmSwapSideBySide(index)}
                                                            title="Click to replace the current index definition with the replacement index"
                                                        >
                                                            {swapNowProgress.includes(index.name) ? (
                                                                <Spinner size={"sm"} />
                                                            ) : (
                                                                <i className="icon-force me-1" />
                                                            )}{" "}
                                                            Swap now
                                                        </Button>
                                                    </div>
                                                </Card>
                                            )}
                                            {replacement && (
                                                <IndexPanel
                                                    setPriority={(p) => setIndexPriority(replacement, p)}
                                                    setLockMode={(l) => setIndexLockMode(replacement, l)}
                                                    globalIndexingStatus={globalIndexingStatus}
                                                    resetIndex={() => resetIndex(replacement)}
                                                    openFaulty={(location: databaseLocationSpecifier) =>
                                                        openFaulty(replacement, location)
                                                    }
                                                    enableIndexing={() => toggleDisableIndexes(true, [replacement])}
                                                    disableIndexing={() => toggleDisableIndexes(false, [replacement])}
                                                    pauseIndexing={() => togglePauseIndexes(false, [replacement])}
                                                    resumeIndexing={() => togglePauseIndexes(true, [replacement])}
                                                    index={replacement}
                                                    database={database}
                                                    deleteIndex={() => confirmDeleteIndexes(database, [replacement])}
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

            {resetIndexConfirm && (
                <ConfirmResetIndex
                    {...resetIndexConfirm}
                    toggle={() => setResetIndexConfirm(null)}
                    onConfirm={() => onResetIndexConfirm(resetIndexConfirm.index)}
                />
            )}
        </>
    );
}
