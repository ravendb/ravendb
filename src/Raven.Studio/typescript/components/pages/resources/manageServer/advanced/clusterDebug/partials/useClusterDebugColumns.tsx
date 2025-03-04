import { ColumnDef } from "@tanstack/react-table";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import genUtils from "common/generalUtils";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import classNames from "classnames";

export function useClusterDebugColumns(
    availableWidth: number,
    commitIndex: number,
    showInlinePreview: (logIndex: number) => void,
    deleteEntry: (logIndex: number) => void
) {
    const columns: ColumnDef<Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry>[] = [
        {
            header: "Preview",
            cell: (context) => (
                <Button
                    title="Show item preview"
                    variant="secondary"
                    onClick={() => showInlinePreview(context.row.original.Index)}
                    className={classNames({ invisible: context.row.original.SizeInBytes === 0 })}
                >
                    <Icon icon="preview" margin="m-0" />
                </Button>
            ),
            size: 70,
        },
        {
            header: "Index",
            accessorKey: "Index",
            cell: CellValueWrapper,
            size: 100,
        },
        {
            id: "commandType",
            header: "Command Type",
            accessorKey: "CommandType",
            cell: CellValueWrapper,
            size: 0, // see code below - we do here flex-grow
        },
        {
            header: "Created",
            accessorKey: "CreateAt",
            cell: CellValueWrapper,
            size: 250,
        },
        {
            header: "Size",
            accessorFn: (row) => genUtils.formatBytesToSize(row.SizeInBytes),
            cell: CellValueWrapper,
            size: 100,
        },
        {
            header: "Term",
            accessorKey: "Term",
            cell: CellValueWrapper,
            size: 70,
        },
        {
            header: "Status",
            accessorFn: (row) => (row.Index <= commitIndex ? "Commited" : "Appended"),
            cell: CellValueWrapper,
            size: 150,
        },
        {
            header: "Delete",
            cell: (context) => (
                <Button
                    title="Delete Log Entry"
                    variant="danger"
                    onClick={() => deleteEntry(context.row.original.Index)}
                    className={classNames({
                        invisible: context.row.original.Index <= commitIndex,
                    })}
                >
                    <Icon icon="trash" margin="m-0" />
                </Button>
            ),
            size: 80,
        },
    ];

    const allocatedSize = columns.reduce((p, c) => p + c.size, 0);
    const remainingSize = availableWidth - allocatedSize - 32; //TODO: don't hardcode padding?
    const commandTypeColumn = columns.find((x) => x.id === "commandType");
    if (commandTypeColumn) {
        commandTypeColumn.size = remainingSize;
    }

    return {
        columns,
    };
}
