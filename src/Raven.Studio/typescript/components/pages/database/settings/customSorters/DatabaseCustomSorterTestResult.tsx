import { useReactTable, getCoreRowModel, getSortedRowModel, ColumnDef } from "@tanstack/react-table";
import SizeGetter from "components/common/SizeGetter";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { useDocumentColumnsProvider } from "components/common/virtualTable/columnProviders/useDocumentColumnsProvider";
import TableDisplaySettings from "components/common/virtualTable/commonComponents/columnsSelect/TableDisplaySettings";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useState } from "react";
import { Button, Badge } from "reactstrap";
import document from "models/database/documents/document";

type TestResultTab = "results" | "diagnostics";

interface DiagnosticsItem {
    Message: string;
}

interface DatabaseCustomSorterTestResultProps {
    result: pagedResultExtended<document>;
}

interface DatabaseCustomSorterTestResultWithSizeProps extends DatabaseCustomSorterTestResultProps {
    availableWidth: number;
}

export default function DatabaseCustomSorterTestResult(props: DatabaseCustomSorterTestResultProps) {
    return (
        <SizeGetter
            render={({ width }) => <DatabaseCustomSorterTestResultWithSize {...props} availableWidth={width} />}
        />
    );
}

function DatabaseCustomSorterTestResultWithSize({
    result,
    availableWidth,
}: DatabaseCustomSorterTestResultWithSizeProps) {
    const [currentTab, setCurrentTab] = useState<TestResultTab>("results");

    const tableBodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);

    const { columnDefs: resultsColumnDefs, initialColumnVisibility } = useDocumentColumnsProvider({
        documents: result?.items || [],
        availableWidth: tableBodyWidth,
        hasPreview: true,
    });

    const [columnVisibility, setColumnVisibility] = useState<Record<string, boolean>>(initialColumnVisibility);

    const resultsTable = useReactTable({
        columns: resultsColumnDefs,
        data: result?.items || [],
        columnResizeMode: "onChange",
        state: {
            columnVisibility,
        },
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        onColumnVisibilityChange: setColumnVisibility,
    });

    const diagnosticsTable = useReactTable({
        columns: getDiagnosticsColumns(tableBodyWidth),
        data: result?.additionalResultInfo?.Diagnostics || [],
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

    return (
        <div>
            <h3>Test Results</h3>
            {currentTab === "results" && (
                <>
                    <span className="text-muted small">Displaying up to 128 results</span>
                    <div className="d-flex justify-content-end mb-2">
                        <TableDisplaySettings table={resultsTable} />
                    </div>
                    <VirtualTable table={resultsTable} heightInPx={400} />
                </>
            )}
            {currentTab === "diagnostics" && <VirtualTable table={diagnosticsTable} heightInPx={400} />}

            <div className="d-flex mt-2 gap-2">
                <Button
                    size="sm"
                    className="rounded-pill"
                    onClick={() => setCurrentTab("results")}
                    active={currentTab === "results"}
                >
                    Results
                    <Badge color="primary" className="ms-1">
                        {result.items.length}
                    </Badge>
                </Button>
                <Button
                    size="sm"
                    className="rounded-pill"
                    onClick={() => setCurrentTab("diagnostics")}
                    active={currentTab === "diagnostics"}
                >
                    Diagnostics
                    <Badge color="primary" className="ms-1">
                        {result.additionalResultInfo?.Diagnostics?.length}
                    </Badge>
                </Button>
            </div>
        </div>
    );
}

const getDiagnosticsColumns = (tableWidth = 0): ColumnDef<DiagnosticsItem>[] => {
    return [
        {
            header: "Message",
            accessorFn: (x) => x,
            cell: CellWithCopyWrapper,
            size: tableWidth,
        },
    ];
};
