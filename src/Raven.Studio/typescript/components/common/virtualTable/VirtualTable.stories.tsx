import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import VirtualTable from "./VirtualTable";
import document from "models/database/documents/document";
import { useDocumentColumnsProvider } from "./columnProviders/useDocumentColumnsProvider";
import { mockStore } from "test/mocks/store/MockStore";
import { useState } from "react";
import { useReactTable, getCoreRowModel, getSortedRowModel, ColumnDef } from "@tanstack/react-table";
import TableDisplaySettings from "./commonComponents/columnsSelect/TableDisplaySettings";
import { FlexGrow } from "components/common/FlexGrow";
import { CellValueWrapper } from "./cells/CellValue";
import VirtualTableWithDynamicLoading from "./VirtualTableWithLazyLoading";
import { useVirtualTableWithLazyLoading } from "./hooks/useVirtualTableWithLazyLoading";

// copied from queryCommand
const selector = (
    results: Raven.Client.Documents.Queries.QueryResult<Array<any>, any>
): pagedResultExtended<document> => ({
    items: results.Results.map((d) => new document(d)),
    totalResultCount: results.CappedMaxResults || results.TotalResults,
    additionalResultInfo: results,
    resultEtag: results.ResultEtag.toString(),
    highlightings: results.Highlightings,
    explanations: results.Explanations,
    timings: results.Timings,
    queryPlan: (results.Timings as any)?.QueryPlan,
    includes: results.Includes,
    includesRevisions: results.RevisionIncludes,
});

const queryCommandResult: pagedResultExtended<document> = selector(require("../../../test/fixtures/query_result.json"));

export default {
    title: "Bits/Virtual Table",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const VirtualTableStory: StoryObj = {
    name: "Default",
    render: () => {
        const { collectionsTracker } = mockStore;
        collectionsTracker.with_Collections();

        return <VirtualTableExample />;
    },
};

function VirtualTableExample() {
    const { columnDefs, initialColumnVisibility } = useDocumentColumnsProvider({
        documents: queryCommandResult.items,
        availableWidth: window.innerWidth,
        hasCheckbox: true,
        hasPreview: true,
        hasFlags: true,
    });

    const [rowSelection, setRowSelection] = useState({});
    const [columnVisibility, setColumnVisibility] = useState<Record<string, boolean>>(initialColumnVisibility);

    const table = useReactTable({
        data: queryCommandResult.items,
        columns: columnDefs,
        columnResizeMode: "onChange",
        state: {
            rowSelection,
            columnVisibility,
        },
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        onRowSelectionChange: setRowSelection,
        onColumnVisibilityChange: setColumnVisibility,
    });

    return (
        <div>
            <div className="d-flex mb-2">
                <FlexGrow />
                <TableDisplaySettings table={table} />
            </div>
            <VirtualTable table={table} heightInPx={400} />
            <hr />
            <h5>Selected Items:</h5>
            <pre>{JSON.stringify(rowSelection, null, 2)}</pre>
        </div>
    );
}

export const VirtualTableWithDynamicLoadingStory: StoryObj = {
    name: "With lazy loading",
    render: VirtualTableWithDynamicLoadingExample,
};

function VirtualTableWithDynamicLoadingExample() {
    const { dataArray, componentProps } = useVirtualTableWithLazyLoading({ fetchData });

    const table = useReactTable({
        defaultColumn: {
            enableSorting: false,
        },
        data: dataArray,
        columns: lazyLoadingColumnDefs,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
    });

    return (
        <div>
            <h2>100M items</h2>
            <VirtualTableWithDynamicLoading {...componentProps} table={table} heightInPx={500} />
        </div>
    );
}

interface Item {
    id: number;
    name: string;
}

// mocked fetcher with 100_000_001 items
function fetchData(skip: number, take: number): Promise<pagedResult<Item>> {
    const items: Item[] = new Array(take).fill(null).map((_, i) => {
        return {
            id: skip + i,
            name: `Item ${skip + i}`,
        };
    });

    return new Promise((resolve) => {
        setTimeout(() => {
            resolve({
                totalResultCount: 100_000_001,
                items,
            });
        }, 500);
    });
}

const lazyLoadingColumnDefs: ColumnDef<Item>[] = [
    {
        header: "Index",
        accessorKey: "id",
        cell: CellValueWrapper,
        size: 300,
    },
    {
        header: "Name",
        accessorKey: "name",
        cell: CellValueWrapper,
        size: 500,
    },
];
