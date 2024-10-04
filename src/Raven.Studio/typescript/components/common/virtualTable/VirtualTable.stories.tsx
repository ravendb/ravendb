import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import VirtualTable from "./VirtualTable";
import document from "models/database/documents/document";
import { useDocumentColumnsProvider } from "./columnProviders/useDocumentColumnsProvider";
import { mockStore } from "test/mocks/store/MockStore";
import { useMemo, useState } from "react";
import { useReactTable, getCoreRowModel, getSortedRowModel, ColumnDef } from "@tanstack/react-table";
import TableDisplaySettings from "./commonComponents/columnsSelect/TableDisplaySettings";
import { FlexGrow } from "components/common/FlexGrow";
import { CellValueWrapper } from "./cells/CellValue";
import { useVirtualTableWithToken } from "components/common/virtualTable/hooks/useVirtualTableWithToken";
import { useVirtualTableWithLazyLoading } from "components/common/virtualTable/hooks/useVirtualTableWithLazyLoading";
import VirtualTableWithLazyLoading from "components/common/virtualTable/VirtualTableWithLazyLoading";

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

export const VirtualTableWithLazyLoadingStory: StoryObj = {
    name: "With lazy loading",
    render: VirtualTableWithLazyLoadingExample,
};

export const VirtualTableWithTokenStory: StoryObj = {
    name: "With token (infinite scroll)",
    render: VirtualTableWithTokenExample,
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

function VirtualTableWithLazyLoadingExample() {
    const { dataPreview, componentProps } = useVirtualTableWithLazyLoading({ fetchData: fetchPagedResultData });

    const table = useReactTable({
        defaultColumn: {
            enableSorting: false,
        },
        data: dataPreview,
        columns: itemColumnDefs,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
    });

    return (
        <div>
            <h2>100M items</h2>
            <VirtualTableWithLazyLoading {...componentProps} table={table} heightInPx={500} />
        </div>
    );
}

function VirtualTableWithTokenExample() {
    const fetchData = useMemo(() => fetchPagedResultWithToken(100), []);

    const { dataArray, componentProps } = useVirtualTableWithToken({ fetchData });

    const table = useReactTable({
        defaultColumn: {
            enableSorting: false,
        },
        columns: itemColumnDefs,
        data: dataArray,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
    });

    return (
        <div>
            <h2>Infinity scroll</h2>
            <VirtualTable {...componentProps} table={table} heightInPx={500} />
        </div>
    );
}

interface Item {
    id: number;
    name: string;
}

// mocked fetcher with 100_000_001 items
function fetchPagedResultData(skip: number, take: number): Promise<pagedResult<Item>> {
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
        }, 200);
    });
}

function fetchPagedResultWithToken(take: number): () => Promise<pagedResultWithToken<Item>> {
    const initialTake = take;
    let lastFetchedIndex = 0;

    return () => {
        const items: Item[] = new Array(initialTake).fill(null).map((_, i) => {
            return {
                id: lastFetchedIndex + i,
                name: `Item ${lastFetchedIndex + i}`,
            };
        });

        lastFetchedIndex += initialTake;

        return new Promise((resolve) => {
            setTimeout(() => {
                resolve({
                    totalResultCount: 100_000_001,
                    items,
                    continuationToken: "continuationToken",
                });
            }, 200);
        });
    };
}

const itemColumnDefs: ColumnDef<Item>[] = [
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
