import { CellContext, ColumnDef } from "@tanstack/react-table";
import CellValue, { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import DocumentIdentitiesModal from "components/pages/database/documents/identities/DocumentIdentitiesModal";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import useBoolean from "hooks/useBoolean";
import { AddIdentitiesFormData } from "components/pages/database/documents/identities/DocumentIdentitiesValidation";

export function useDocumentIdentitiesColumns(availableWidth: number, reload: () => void) {
    const databaseAccessWrite = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const identitiesColumns: ColumnDef<AddIdentitiesFormData>[] = [
        {
            header: "Document ID Prefix",
            accessorKey: "prefix",
            cell: CellValuePrefixWrapper,
            size: getSize(databaseAccessWrite ? 46 : 50),
        },
        {
            header: "Latest value",
            accessorKey: "value",
            cell: CellValueWrapper,
            size: getSize(databaseAccessWrite ? 46 : 50),
        },
    ];

    if (databaseAccessWrite) {
        identitiesColumns.push({
            id: "actions",
            header: "Edit",
            cell: (props) => <CellValueButtonWrapper refetch={reload} {...props} />,
            size: getSize(8),
        });
    }

    return {
        identitiesColumns,
    };
}

type CellValuePrefixWrapperProps = Pick<
    CellContext<AddIdentitiesFormData, AddIdentitiesFormData["prefix"]>,
    "getValue"
>;

function CellValuePrefixWrapper({ getValue }: CellValuePrefixWrapperProps) {
    let value = getValue();

    if (value.endsWith("|")) {
        value = value.slice(0, -1);
    }

    return <CellValue value={value} title={value} />;
}

type CellValueButtonWrapperProps = CellContext<AddIdentitiesFormData, unknown> & { refetch: () => void };

function CellValueButtonWrapper(args: CellValueButtonWrapperProps) {
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(false);

    return (
        <>
            <Button
                variant="secondary"
                onClick={toggleIsOpen}
                className="d-flex align-items-center h-100"
                title="Edit identity settings"
            >
                <Icon icon="edit" margin="me-0" />
            </Button>
            <DocumentIdentitiesModal
                refetch={args.refetch}
                show={isOpen}
                defaultValues={args.row.original}
                onHide={toggleIsOpen}
            />
        </>
    );
}
