import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "hooks/useBoolean";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { useState } from "react";
import { IndexErrorsPanelProps } from "components/pages/database/indexes/errors/IndexErrorsPanel";
import messagePublisher from "common/messagePublisher";
import { indexErrorsUtils } from "components/pages/database/indexes/errors/IndexErrorsUtils";
import { ColumnFilter } from "@tanstack/react-table";
import genUtils from "common/generalUtils";

export function useIndexErrorsPanel({ errorItem, table, asyncFetchAllErrorCount }: IndexErrorsPanelProps) {
    const { indexesService } = useServices();
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { value: panelCollapsed, toggle: togglePanelCollapsed } = useBoolean(true);
    const { value: isDeleteModalOpen, setTrue: openDeleteModal, setFalse: closeDeleteModal } = useBoolean(false);
    const [mappedIndexErrors, setMappedIndexErrors] = useState<IndexErrorPerDocument[]>();

    const selectedIndexes: ColumnFilter | undefined = table.getState().columnFilters.find((x) => x.id === "IndexName");
    const selectedErrors = (selectedIndexes?.value as string[] | undefined) ?? [];

    const hasErrors = errorItem.totalErrorCount > 0;

    const asyncClearSelectedIndexErrors = useAsyncCallback(
        async () => indexesService.clearIndexErrors(selectedErrors, dbName, errorItem.location),
        {
            onSuccess: async () => {
                messagePublisher.reportSuccess(
                    `Successfully cleared index errors for ${genUtils.formatLocation(errorItem.location)}!`
                );
                closeDeleteModal();
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

    return {
        selectedErrors,
        isDeleteModalOpen,
        openDeleteModal,
        closeDeleteModal,
        asyncClearSelectedIndexErrors,
        mappedIndexErrors,
        hasErrors,
        asyncFetchErrorDetails,
        newestDate,
        panelCollapsed,
        togglePanelCollapsed,
    };
}
