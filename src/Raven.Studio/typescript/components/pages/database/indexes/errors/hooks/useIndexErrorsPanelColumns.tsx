import { useAppSelector } from "components/store";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { CellContext, ColumnDef } from "@tanstack/react-table";
import CellValue, { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import { useAppUrls } from "hooks/useAppUrls";
import { CellWithCopy, CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { Button } from "reactstrap";
import { Icon } from "components/common/Icon";
import IndexErrorsModal from "components/pages/database/indexes/errors/IndexErrorsModal";
import useBoolean from "hooks/useBoolean";

const defaultCellSize = 90 / 5;

export function useIndexErrorsPanelColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const indexErrorsPanelColumns: ColumnDef<IndexErrorPerDocument>[] = [
        {
            header: "Show",
            cell: CellValueButtonWrapper,
            size: getSize(10),
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
            cell: CellWithCopyWrapper,
            size: getSize(defaultCellSize),
        },
        {
            header: "Action",
            accessorKey: "Action",
            cell: CellValueWrapper,
            size: getSize(defaultCellSize),
            filterFn: "arrIncludesSome",
            enableColumnFilter: false,
        },
        {
            header: "Error",
            accessorKey: "Error",
            cell: CellWithCopyWrapper,
            size: getSize(defaultCellSize),
        },
    ];

    return { indexErrorsPanelColumns };
}

type HyperLinkDocumentCellValueProps = Pick<
    CellContext<IndexErrorPerDocument, IndexErrorPerDocument["Document"]>,
    "getValue"
>;

const HyperLinkDocumentCellValue = ({ getValue }: HyperLinkDocumentCellValueProps) => {
    return <CellDocumentValue value={getValue()} databaseName="test" hasHyperlinkForIds />;
};

type HyperlinkIndexCellValueProps = Pick<
    CellContext<IndexErrorPerDocument, IndexErrorPerDocument["IndexName"]>,
    "getValue"
>;

const HyperlinkIndexCellValue = ({ getValue }: HyperlinkIndexCellValueProps) => {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { appUrl } = useAppUrls();

    const getLinkToIndex = (cellValue: IndexErrorPerDocument["IndexName"]): string => {
        if (typeof cellValue !== "string") {
            return null;
        }

        return appUrl.forEditIndex(getValue(), databaseName);
    };

    const editIndexLink = getLinkToIndex(getValue());
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

const CellValueButtonWrapper = (args: CellValueButtonWrapperProps) => {
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(false);

    return (
        <>
            <Button onClick={toggleIsOpen}>
                <Icon icon="preview" margin="m-0" />
            </Button>
            <IndexErrorsModal
                isOpen={isOpen}
                toggleModal={toggleIsOpen}
                errorDetails={args.row}
                dataLength={args.table.options.data.length}
                getRow={args.table.getRow}
            />
        </>
    );
};
