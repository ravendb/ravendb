import React from "react";
import { ColumnDef, getCoreRowModel, useReactTable } from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import DateFormatterCell from "components/common/virtualTable/cells/CellDateFormatter";
import genUtils from "common/generalUtils";

interface ClusterSnapshotInstallationProps {
    messages: Raven.Server.Rachis.RachisDebugMessage[];
    availableWidth: number;
}

export default function ClusterSnapshotInstallation(props: ClusterSnapshotInstallationProps) {
    const { messages, availableWidth } = props;

    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const messagesColumns: ColumnDef<Raven.Server.Rachis.RachisDebugMessage>[] = [
        {
            header: "Time",
            accessorKey: "At",
            cell: (props) => <DateFormatterCell {...props} displayFormat={genUtils.dateFormat} />,
            size: getSize(25),
        },
        {
            header: "Message",
            accessorKey: "Message",
            cell: CellWithCopyWrapper,
            size: getSize(75),
        },
    ];

    const messagesTable = useReactTable({
        defaultColumn: {
            enableColumnFilter: false,
        },
        initialState: {
            sorting: [
                {
                    id: "At",
                    desc: true,
                },
            ],
        },
        columns: messagesColumns,
        data: messages,
        getCoreRowModel: getCoreRowModel(),
    });

    return <VirtualTable heightInPx={400} table={messagesTable} isLoading={false} />;
}
