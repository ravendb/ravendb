import { useMemo, useState } from "react";
import { CellContext, ColumnDef } from "@tanstack/react-table";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { Button } from "reactstrap";
import { Icon } from "components/common/Icon";
import DocumentIdentitiesAddModal from "components/pages/database/documents/identities/DocumentIdentitiesAddModal";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

type IdentitiesItem = {
    prefix: string;
    value: number;
};

export function useDocumentIdentitiesColumns(availableWidth: number) {
    const databaseAccessWrite = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const identitiesColumns: ColumnDef<IdentitiesItem>[] = useMemo(
        () => [
            {
                header: "Document ID Prefix",
                accessorKey: "prefix",
                cell: CellValueWrapper,
                size: getSize(22),
            },
            {
                header: "Latest value",
                accessorKey: "value",
                cell: CellValueWrapper,
                size: getSize(22),
            },
        ],
        [getSize]
    );

    if (databaseAccessWrite) {
        identitiesColumns.push({
            id: "actions",
            header: "Edit",
            cell: CellValueButtonWrapper,
            size: getSize(6),
        });
    }

    return {
        identitiesColumns,
    };
}

function CellValueButtonWrapper(args: CellContext<IdentitiesItem, unknown>) {
    const [isOpen, setIsOpen] = useState(false);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const toggleModal = (value: boolean) => setIsOpen(value);
    return (
        <>
            <Button onClick={() => setIsOpen(!isOpen)}>
                <Icon icon="edit" margin="me-0" />
            </Button>
            <DocumentIdentitiesAddModal
                isOpen={isOpen}
                defaultValues={args.row.original}
                databaseName={databaseName}
                toggleModal={toggleModal}
            />
        </>
    );
}
