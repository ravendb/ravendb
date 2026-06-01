import { useAppSelector } from "components/store";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { CellContext, ColumnDef, Row, Table as TanstackTable } from "@tanstack/react-table";
import CellValue, { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import { useAppUrls } from "hooks/useAppUrls";
import { CellWithCopy } from "components/common/virtualTable/cells/CellWithCopy";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import IndexErrorsSheet from "components/pages/database/indexes/errors/IndexErrorsSheet";
import { OpenSheetOptions, useViewSheet } from "components/common/splitView/ViewSheet";
import React from "react";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

const indexErrorsSheetConfig: Pick<OpenSheetOptions, "initialWidth" | "minWidth" | "maxWidth"> = {
    initialWidth: "40%",
    minWidth: "25%",
    maxWidth: "60%",
};

function openIndexErrorsSheet(
    open: ReturnType<typeof useViewSheet>["open"],
    table: TanstackTable<IndexErrorPerDocument>,
    row: Row<IndexErrorPerDocument>
) {
    const allRows = table.getRowModel().rows;
    const currentIndex = allRows.findIndex((r) => r.id === row.id);
    open({
        component: (
            <IndexErrorsSheet
                errorDetails={row}
                allRows={allRows}
                initialIndex={currentIndex >= 0 ? currentIndex : 0}
            />
        ),
        ...indexErrorsSheetConfig,
    });
}

const defaultCellSize = 95 / 5;

export function useIndexErrorsPanelColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const indexErrorsPanelColumns: ColumnDef<IndexErrorPerDocument>[] = [
        {
            header: "Show",
            cell: CellValueButtonWrapper,
            size: 70,
        },
        {
            header: "Index Name",
            accessorKey: "IndexName",
            cell: HyperlinkIndexCellValue,
            size: getSize(defaultCellSize),
            filterFn: "arrIncludesSome",
            enableColumnFilter: false,
        },
        {
            header: "Document ID",
            accessorKey: "Document",
            cell: HyperLinkDocumentCellValue,
            size: getSize(defaultCellSize),
        },
        {
            header: "Date",
            accessorKey: "LocalTime",
            cell: CellValueRelativeTimeWrapper,
            size: getSize(defaultCellSize),
        },
        {
            header: "Action",
            accessorKey: "Action",
            cell: CellValueWrapper,
            size: getSize(defaultCellSize / 2),
            filterFn: "arrIncludesSome",
            enableColumnFilter: false,
        },
        {
            header: "Error",
            accessorKey: "Error",
            cell: IndexErrorsCellWithCopyWrapper,
            size: getSize(defaultCellSize * 1.5),
        },
    ];

    return { indexErrorsPanelColumns };
}

type HyperLinkDocumentCellValueProps = Pick<
    CellContext<IndexErrorPerDocument, IndexErrorPerDocument["Document"]>,
    "getValue" | "table"
>;

const HyperLinkDocumentCellValue = ({ getValue, table }: HyperLinkDocumentCellValueProps) => {
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);
    const disableLinks = (table.options.meta as { disableLinks?: boolean })?.disableLinks;

    return <CellDocumentValue value={getValue()} databaseName={dbName} hasHyperlinkForIds={!disableLinks} />;
};

type HyperlinkIndexCellValueProps = Pick<
    CellContext<IndexErrorPerDocument, IndexErrorPerDocument["IndexName"]>,
    "getValue" | "table"
>;

const HyperlinkIndexCellValue = ({ getValue, table }: HyperlinkIndexCellValueProps) => {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { appUrl } = useAppUrls();
    const disableLinks = (table.options.meta as { disableLinks?: boolean })?.disableLinks;

    const getLinkToIndex = (cellValue: IndexErrorPerDocument["IndexName"]): string => {
        if (typeof cellValue !== "string") {
            return null;
        }

        return appUrl.forEditIndex(getValue(), databaseName);
    };

    const editIndexLink = disableLinks ? null : getLinkToIndex(getValue());
    if (editIndexLink) {
        return (
            <CellWithCopy value={getValue()}>
                <a href={editIndexLink}>{String(getValue())}</a>
            </CellWithCopy>
        );
    }

    return (
        <CellWithCopy value={getValue()}>
            <CellValue value={getValue()} />
        </CellWithCopy>
    );
};

type CellValueButtonWrapperProps = CellContext<IndexErrorPerDocument, unknown>;

const CellValueButtonWrapper = ({ row, table }: CellValueButtonWrapperProps) => {
    const { open } = useViewSheet();

    return (
        <Button variant="link" onClick={() => openIndexErrorsSheet(open, table, row)}>
            <Icon icon="preview" margin="m-0" />
        </Button>
    );
};

type CellValueRelativeTimeWrapperProps = CellContext<IndexErrorPerDocument, IndexErrorPerDocument["LocalTime"]>;

const CellValueRelativeTimeWrapper = ({ getValue, row }: CellValueRelativeTimeWrapperProps) => {
    const rowData = row.original;

    return (
        <PopoverWithHoverWrapper
            message={
                <>
                    <div className="index-errors-details-tooltip__container">
                        <b>UTC: </b>
                        <time>{rowData.Timestamp}</time>
                    </div>
                    <div className="index-errors-details-tooltip__container">
                        <b>Relative: </b>
                        <time>{rowData.RelativeTime}</time>
                    </div>
                </>
            }
        >
            <CellValue value={getValue()} />
        </PopoverWithHoverWrapper>
    );
};
type IndexErrorsCellWithCopyWrapperProps = CellContext<IndexErrorPerDocument, unknown>;

const IndexErrorsCellWithCopyWrapper = ({ getValue, row, table }: IndexErrorsCellWithCopyWrapperProps) => {
    const { open } = useViewSheet();

    const additionalButtons = (
        <Button size="sm" onClick={() => openIndexErrorsSheet(open, table, row)} title="Show error details">
            <Icon icon="preview" margin="m-0" />
        </Button>
    );

    return (
        <CellWithCopy additionalButtons={additionalButtons} value={getValue()}>
            <CellValue value={getValue()} />
        </CellWithCopy>
    );
};
