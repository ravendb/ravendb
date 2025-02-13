import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "hooks/useBoolean";
import useConfirm from "components/common/ConfirmDialog";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { IndexErrorsPanelProps } from "components/pages/database/indexes/errors/IndexErrorsPanel";
import messagePublisher from "common/messagePublisher";
import { indexErrorsUtils } from "components/pages/database/indexes/errors/IndexErrorsUtils";
import { ColumnFilter } from "@tanstack/react-table";
import RichAlert from "components/common/RichAlert";
import genUtils from "common/generalUtils";

export function useIndexErrorsPanel({ errorItem, table, asyncFetchAllErrorCount }: IndexErrorsPanelProps) {
    const { indexesService } = useServices();
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { value: panelCollapsed, toggle: togglePanelCollapsed } = useBoolean(true);
    const [mappedIndexErrors, setMappedIndexErrors] = useState<IndexErrorPerDocument[]>();
    const confirm = useConfirm();

    const selectedIndexes: ColumnFilter | undefined = table.getState().columnFilters.find((x) => x.id === "IndexName");
    const selectedErrors = (selectedIndexes?.value as string[] | undefined) ?? [];

    const hasErrors = errorItem.totalErrorCount > 0;

    const handleClearSelectedIndexErrorsForNode = useAsyncCallback(
        async () => indexesService.clearIndexErrors(selectedErrors, dbName, errorItem.location),
        {
            onSuccess: async () => {
                messagePublisher.reportSuccess(
                    `Successfully cleared index errors for ${genUtils.formatLocation(errorItem.location)}!`
                );
                await asyncFetchAllErrorCount.execute();
            },
        }
    );

    const asyncFetchErrorDetails = useAsync(
        () => indexesService.getIndexErrorDetails(dbName, errorItem.location),
        [asyncFetchAllErrorCount.status],
        {
            onSuccess: (resultDto) => setMappedIndexErrors(indexErrorsUtils.mapItems(resultDto)),
        }
    );

    const newestDate = indexErrorsUtils.findNearestTimestamp(asyncFetchErrorDetails.result ?? []);

    const handleClearErrors: () => Promise<void> = async () => {
        const isConfirmed = await confirm({
            title: (
                <IndexErrorsModalTitle
                    nodeTag={errorItem.location.nodeTag}
                    shardNumber={errorItem.location.shardNumber}
                />
            ),
            message: <IndexErrorsModalBody selectedErrors={selectedErrors} />,
            actionColor: "warning",
            icon: "trash",
            confirmText: "Clear errors",
        });

        if (!isConfirmed) {
            return null;
        }

        await handleClearSelectedIndexErrorsForNode.execute();
    };

    return {
        handleClearErrors,
        mappedIndexErrors,
        hasErrors,
        asyncFetchErrorDetails,
        newestDate,
        panelCollapsed,
        togglePanelCollapsed,
    };
}

interface IndexErrorsModalTitleProps {
    shardNumber?: number;
    nodeTag: string;
}

function IndexErrorsModalTitle({ shardNumber, nodeTag }: IndexErrorsModalTitleProps) {
    return (
        <span>
            Clear errors for <Icon icon="node" color="node" margin="m-0" /> <strong>{nodeTag}</strong>{" "}
            {shardNumber != null && (
                <>
                    <Icon icon="shard" color="shard" margin="m-0" /> <strong>#{shardNumber}</strong>
                </>
            )}
        </span>
    );
}

interface IndexErrorsModalBodyProps {
    selectedErrors: string[];
}

function IndexErrorsModalBody({ selectedErrors }: IndexErrorsModalBodyProps) {
    return (
        <div>
            {selectedErrors.length === 0 ? (
                <>
                    <span>
                        Errors will be cleared for <strong>ALL</strong> indexes.{" "}
                    </span>
                    {selectedErrors.length === 0 && (
                        <span>
                            If you want to clear errors for <b>specific indexes</b>, select them in dropdown.
                        </span>
                    )}
                </>
            ) : (
                <>
                    You&#39;re clearing errors for <b>{selectedErrors.length}</b> indexes:
                    <ul>
                        {selectedErrors.map((error) => (
                            <li title={error} className="text-truncate">
                                <b>{error}</b>
                            </li>
                        ))}
                    </ul>
                </>
            )}
            <RichAlert variant="info" className="mt-3">
                While the current indexing errors will be cleared, an index with an <b>Error state</b> will not be set
                back to <b>Normal</b> state.
            </RichAlert>
        </div>
    );
}
