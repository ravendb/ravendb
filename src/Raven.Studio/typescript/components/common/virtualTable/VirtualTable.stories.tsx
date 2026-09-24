import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import VirtualTable from "./VirtualTable";
import document from "models/database/documents/document";
import { useDocumentColumnsProvider } from "./columnProviders/useDocumentColumnsProvider";
import { mockStore } from "test/mocks/store/MockStore";
import { useMemo, useState } from "react";
import {
    useReactTable,
    getCoreRowModel,
    getSortedRowModel,
    ColumnDef,
    getFilteredRowModel,
    ColumnFiltersState,
    ColumnPinningState,
} from "@tanstack/react-table";
import TableDisplaySettings from "./commonComponents/columnsSelect/TableDisplaySettings";
import { FlexGrow } from "components/common/FlexGrow";
import { CellValueWrapper } from "./cells/CellValue";
import { useVirtualTableWithToken } from "components/common/virtualTable/hooks/useVirtualTableWithToken";
import { useLazyRows } from "components/common/virtualTable/hooks/useLazyRows";
import { LazyFetchMode } from "components/common/virtualTable/utils/LazyRowsLoader";
import { lazyTableOptions } from "components/common/virtualTable/utils/lazyTableUtils";
import LazyVirtualTable from "components/common/virtualTable/LazyVirtualTable";
import { Switch } from "components/common/Checkbox";

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
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=15-838",
        },
    },
} satisfies Meta;

export const VirtualTableStory: StoryObj = {
    name: "Default",
    render: () => {
        const { collectionsTracker } = mockStore;
        collectionsTracker.with_Collections();

        return <VirtualTableExample />;
    },
};

interface LazyLoadingStoryArgs {
    totalCount: number;
    fetchMode: LazyFetchMode;
    fetchDelayInMs: number;
    minFetchCount: number;
    heightInPx: number;
}

export const LazyVirtualTableStory: StoryObj<LazyLoadingStoryArgs> = {
    name: "With lazy loading",
    render: (args) => <LazyVirtualTableExample {...args} />,
    args: {
        totalCount: 100_000_001,
        fetchMode: "skipTake",
        fetchDelayInMs: 200,
        minFetchCount: 100,
        heightInPx: 500,
    },
    argTypes: {
        fetchMode: {
            control: "radio",
            options: ["skipTake", "continuationToken"] satisfies LazyFetchMode[],
        },
    },
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
    const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
    const [columnOrder, setColumnOrder] = useState<string[]>([]);
    const [columnPinning, setColumnPinning] = useState<ColumnPinningState>({});

    const table = useReactTable({
        data: queryCommandResult.items,
        columns: columnDefs,
        columnResizeMode: "onChange",
        state: {
            rowSelection,
            columnVisibility,
            columnFilters,
            columnOrder,
            columnPinning,
        },
        onColumnFiltersChange: setColumnFilters,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        onRowSelectionChange: setRowSelection,
        onColumnVisibilityChange: setColumnVisibility,
        onColumnOrderChange: setColumnOrder,
        onColumnPinningChange: setColumnPinning,
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
            <h5>Column filter:</h5>
            <pre>{JSON.stringify(columnFilters, null, 2)}</pre>
            <h5>Column order:</h5>
            <pre>{JSON.stringify(columnOrder, null, 2)}</pre>
            <h5>Column pinning:</h5>
            <pre>{JSON.stringify(columnPinning, null, 2)}</pre>
            <h5>Column visibility:</h5>
            <pre>{JSON.stringify(columnVisibility, null, 2)}</pre>
        </div>
    );
}

function LazyVirtualTableExample({
    totalCount,
    fetchMode,
    fetchDelayInMs,
    minFetchCount,
    heightInPx,
}: LazyLoadingStoryArgs) {
    const fetchData = useMemo(() => createLazyLoadingFetcher(totalCount, fetchDelayInMs), [totalCount, fetchDelayInMs]);

    const lazyRows = useLazyRows({ fetchData, fetchMode, minFetchCount, reloadDependencies: [fetchData] });
    const [isPaginated, setIsPaginated] = useState(false);

    const table = useReactTable({
        ...lazyTableOptions,
        data: lazyRows.data,
        columns: itemColumnDefs,
        getCoreRowModel: getCoreRowModel(),
    });

    return (
        <div className="d-flex flex-column" style={{ height: heightInPx }}>
            <div className="d-flex align-items-center gap-3 mb-2">
                <h2 className="m-0">{totalCount.toLocaleString()} items</h2>
                <Switch selected={isPaginated} toggleSelection={() => setIsPaginated(!isPaginated)} color="primary">
                    Pagination
                </Switch>
            </div>
            <LazyVirtualTable
                table={table}
                lazyRows={lazyRows}
                isPaginated={isPaginated}
                onIsPaginatedChange={setIsPaginated}
                heightInPx={heightInPx}
            />
        </div>
    );
}

function VirtualTableWithTokenExample() {
    const fetchData = useMemo(() => fetchPagedResultWithToken(100), []);

    const { dataArray, componentProps } = useVirtualTableWithToken({ fetchData });

    const table = useReactTable({
        defaultColumn: {
            enableSorting: false,
            enableColumnFilter: false,
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

function createItems(skip: number, take: number): Item[] {
    return new Array(take).fill(null).map((_, i) => ({
        id: skip + i,
        name: `Item ${skip + i}`,
    }));
}

// mocked fetcher supporting both (skip, take) and continuation token requests
// the continuation token encodes the index of the next item to fetch
function createLazyLoadingFetcher(totalCount: number, delayInMs: number) {
    return (skip: number, take: number, continuationToken?: string): Promise<pagedResultWithToken<Item>> => {
        const start = continuationToken ? Number(continuationToken) : skip;
        const count = Math.max(0, Math.min(take, totalCount - start));
        const next = start + count;

        return new Promise((resolve) => {
            setTimeout(() => {
                resolve({
                    totalResultCount: totalCount,
                    items: createItems(start, count),
                    continuationToken: next < totalCount ? String(next) : null,
                });
            }, delayInMs);
        });
    };
}

function fetchPagedResultWithToken(take: number): () => Promise<pagedResultWithToken<Item>> {
    const initialTake = take;
    let lastFetchedIndex = 0;

    return () => {
        const items = createItems(lastFetchedIndex, initialTake);

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
