import { ColumnDef } from "@tanstack/react-table";
import genUtils from "common/generalUtils";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";

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
            cell: CellWithCopyWrapper,
            size: 100,
        },
        {
            id: "commandType",
            header: "Command Type",
            accessorKey: "CommandType",
            cell: CellWithCopyWrapper,
            size: 0, // see code below - we do here flex-grow
        },
        {
            header: "Created",
            accessorKey: "CreateAt",
            cell: CellWithCopyWrapper,
            size: 250,
        },
        {
            header: "Size",
            accessorFn: (row) => genUtils.formatBytesToSize(row.SizeInBytes),
            cell: CellWithCopyWrapper,
            size: 100,
        },
        {
            header: "Term",
            accessorKey: "Term",
            cell: CellWithCopyWrapper,
            size: 70,
        },
        {
            header: "Status",
            accessorFn: (row) => (row.Index <= commitIndex ? "Commited" : "Appended"),
            cell: CellWithCopyWrapper,
            size: 150,
        },
        {
            header: "Delete",
            cell: (context) => (
                <Button
                    title="Delete Log Entry"
                    variant="danger"
                    onClick={() => deleteEntry(context.row.original.Index)}
                >
                    <Icon icon="trash" margin="m-0" />
                </Button>
            ),
            size: 80,
        },
    ];

    const allocatedSize = columns.reduce((p, c) => p + c.size, 0);
    const remainingSize = availableWidth - allocatedSize - padding;
    const commandTypeColumn = columns.find((x) => x.id === "commandType");
    if (commandTypeColumn) {
        commandTypeColumn.size = remainingSize;
    }

    return {
        columns,
    };
}

const padding = 32;
