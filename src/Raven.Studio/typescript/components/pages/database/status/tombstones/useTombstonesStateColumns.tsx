import { useMemo } from "react";
import { Getter, ColumnDef } from "@tanstack/react-table";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import SubscriptionInfoExtended = Raven.Server.Documents.TombstoneCleaner.TombstonesState.SubscriptionInfoExtended;

export function useTombstonesStateColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const collectionsColumns: ColumnDef<TombstoneItem>[] = useMemo(
        () => [
            {
                accessorKey: "Collection",
                cell: CellValueWrapper,
                size: getSize(28),
            },
            {
                header: "Document Task",
                accessorFn: (x) => x.Documents.Component,
                cell: CellValueWrapper,
                size: getSize(16),
            },
            {
                header: "Document Etag",
                accessorFn: (x) => x.Documents.Etag,
                cell: CellEtagWrapper,
                size: getSize(8),
            },
            {
                header: "Time Series Task",
                accessorFn: (x) => x.TimeSeries.Component,
                cell: CellValueWrapper,
                size: getSize(16),
            },
            {
                header: "Time Series Etag",
                accessorFn: (x) => x.TimeSeries.Etag,
                cell: CellEtagWrapper,
                size: getSize(8),
            },
            {
                header: "Counter Task",
                accessorFn: (x) => x.Counters.Component,
                cell: CellValueWrapper,
                size: getSize(16),
            },
            {
                header: "Counter Etag",
                accessorFn: (x) => x.Counters.Etag,
                cell: CellEtagWrapper,
                size: getSize(8),
            },
        ],
        [getSize]
    );

    const subscriptionsColumns: ColumnDef<SubscriptionInfoExtended>[] = useMemo(
        () => [
            {
                accessorKey: "Process",
                cell: CellValueWrapper,
                size: getSize(15),
            },
            {
                header: "Name",
                accessorKey: "Identifier",
                cell: CellValueWrapper,
                size: getSize(15),
            },
            {
                header: "Number of tombstones left",
                accessorKey: "NumberOfTombstoneLeft",
                cell: CellValueWrapper,
                size: getSize(10),
            },
            {
                header: "Tombstone types",
                accessorFn: (x) => formatTombstoneTypes(x.Types, x.Process),
                cell: CellValueWrapper,
                size: getSize(25),
            },
            {
                accessorKey: "Collection",
                cell: CellValueWrapper,
                size: getSize(10),
            },
            {
                accessorKey: "Etag",
                cell: CellEtagWrapper,
                size: getSize(10),
            },
            {
                header: "Cleanup status",
                id: "CleanupStatus",
                accessorFn: (x) => (x.NumberOfTombstoneLeft > 0 ? "Blocking" : "Not blocking"),
                cell: CellValueWrapper,
                size: getSize(15),
            },
        ],
        [getSize]
    );

    return {
        collectionsColumns,
        subscriptionsColumns,
        formatEtag,
    };
}

const etagMaxValue = 9223372036854776000; // in general Long.MAX_Value is 9223372036854775807 but we loose precision here

function formatEtag(value: number) {
    if (value === etagMaxValue) {
        return "(max value)";
    }

    return value;
}
function formatTombstoneTypes(
    types: Raven.Server.Documents.TombstoneCleaner.TombstonesState.TombstoneTypes,
    process: Raven.Server.Documents.ITombstoneAware.TombstoneDeletionBlockerType
) {
    if (!types) {
        return "";
    }

    if (process === "Index") {
        return `Documents: ${types.Documents}`;
    }

    return `Documents: ${types.Documents}, TimeSeries: ${types.TimeSeries}, Counters: ${types.Counters}`;
}

function getEtagTitle(etagValue: number) {
    if (etagValue === 0) {
        return "No tombstones can be removed";
    }

    if (etagValue < etagMaxValue) {
        return `Can remove tombstones for Etags <= ${etagValue}`;
    }

    return "Can remove any tombstone";
}

function CellEtagWrapper({ getValue }: { getValue: Getter<number> }) {
    const value = getValue();
    return <CellValue value={formatEtag(value)} title={getEtagTitle(value)} />;
}

function CellValue({ value, title }: { value: unknown; title?: string }) {
    return (
        <span title={title} className={`value-${typeof value}`}>
            {String(value)}
        </span>
    );
}
