import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import IndexUtils from "components/utils/IndexUtils";
import React from "react";
import { Card } from "reactstrap";
import { IndexPanel } from "./IndexPanel";
import { IndexSharedInfo } from "components/models/indexes";
import { Icon } from "components/common/Icon";
import { ResetIndexesData, SwapSideBySideData } from "./useIndexesPage";
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;

export interface IndexesPageListProps {
    indexes: IndexSharedInfo[];
    replacements: IndexSharedInfo[];
    selectedIndexes: string[];
    indexToHighlight: string;
    globalIndexingStatus: "Running";
    resetIndexData: ResetIndexesData;
    swapSideBySideData: SwapSideBySideData;
    setIndexPriority: (index: IndexSharedInfo, priority: IndexPriority) => Promise<void>;
    setIndexLockMode: (index: IndexSharedInfo, lockMode: IndexLockMode) => Promise<void>;
    openFaulty: (index: IndexSharedInfo, location: databaseLocationSpecifier) => Promise<void>;
    startIndexes: (indexes: IndexSharedInfo[]) => Promise<void>;
    disableIndexes: (indexes: IndexSharedInfo[]) => Promise<void>;
    pauseIndexes: (indexes: IndexSharedInfo[]) => Promise<void>;
    confirmDeleteIndexes: (indexes: IndexSharedInfo[]) => Promise<void>;
    toggleSelection: (index: IndexSharedInfo) => void;
    highlightCallback: (node: HTMLElement) => void;
}

export default function IndexesPageList({
    indexes,
    replacements,
    selectedIndexes,
    globalIndexingStatus,
    indexToHighlight,
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
}: IndexesPageListProps) {
    return (
        <>
            {indexes.map((index: any) => {
                const replacement = replacements.find((x) => x.name === IndexUtils.SideBySideIndexPrefix + index.name);
                return (
                    <React.Fragment key={index.name}>
                        <IndexPanel
                            setPriority={(p) => setIndexPriority(index, p)}
                            setLockMode={(l) => setIndexLockMode(index, l)}
                            globalIndexingStatus={globalIndexingStatus}
                            resetIndex={(mode?: Raven.Client.Documents.Indexes.IndexResetMode) =>
                                resetIndexData.openConfirm([index], mode)
                            }
                            openFaulty={(location: databaseLocationSpecifier) => openFaulty(index, location)}
                            startIndexing={() => startIndexes([index])}
                            disableIndexing={() => disableIndexes([index])}
                            pauseIndexing={() => pauseIndexes([index])}
                            index={index}
                            hasReplacement={!!replacement}
                            deleteIndex={() => confirmDeleteIndexes([index])}
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
                                resetIndex={(mode?: Raven.Client.Documents.Indexes.IndexResetMode) =>
                                    resetIndexData.openConfirm([replacement], mode)
                                }
                                openFaulty={(location: databaseLocationSpecifier) => openFaulty(replacement, location)}
                                startIndexing={() => startIndexes([replacement])}
                                disableIndexing={() => disableIndexes([replacement])}
                                pauseIndexing={() => pauseIndexes([replacement])}
                                index={replacement}
                                deleteIndex={() => confirmDeleteIndexes([replacement])}
                                selected={selectedIndexes.includes(replacement.name)}
                                toggleSelection={() => toggleSelection(replacement)}
                                key={replacement.name}
                                ref={undefined}
                            />
                        )}
                    </React.Fragment>
                );
            })}
        </>
    );
}
