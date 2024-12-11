import { ColumnDef } from "@tanstack/react-table";
import classNames from "classnames";
import { RevisionsPreviewResultItem } from "commands/database/documents/getRevisionsPreviewCommand";
import appUrl from "common/appUrl";
import { Checkbox } from "components/common/Checkbox";
import { Icon } from "components/common/Icon";
import { CellWithCopy, CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";

const getColumnDefs = (
    databaseName: string,
    isSharded: boolean,
    tableBodyWidth: number,
    rowSelection: RevisionsPreviewResultItem[],
    setRowSelection: (rows: RevisionsPreviewResultItem[]) => void
): ColumnDef<RevisionsPreviewResultItem>[] => {
    const sizeProvider = virtualTableUtils.getCellSizeProvider(tableBodyWidth - checkboxWidth);

    const getIsSelected = (row: RevisionsPreviewResultItem) => {
        return !!rowSelection.find((x) => x._meta.uniqueId === row._meta.uniqueId);
    };

    const toggleSelection = (row: RevisionsPreviewResultItem) => {
        if (getIsSelected(row)) {
            setRowSelection(rowSelection.filter((x) => x._meta.uniqueId !== row._meta.uniqueId));
        } else {
            setRowSelection([...rowSelection, row]);
        }
    };

    const columns: ColumnDef<RevisionsPreviewResultItem>[] = [
        {
            id: "Checkbox",
            header: "",
            accessorFn: (x) => x,
            cell: ({ getValue }) => {
                const value = getValue<RevisionsPreviewResultItem>();
                return <Checkbox selected={getIsSelected(value)} toggleSelection={() => toggleSelection(value)} />;
            },
            size: checkboxWidth,
            minSize: checkboxWidth,
            enableSorting: false,
            enableHiding: false,
        },
        {
            accessorKey: "Id",
            cell: ({ getValue, row }) => {
                const id = getValue<string>();
                const changeVector = row.getValue<string>("ChangeVector");

                return (
                    <CellWithCopy value={id}>
                        <a href={appUrl.forViewDocumentAtRevision(id, changeVector, databaseName)}>{id}</a>
                    </CellWithCopy>
                );
            },
            size: sizeProvider(20),
        },
        {
            accessorKey: "Collection",
            cell: ({ getValue }) => {
                const collection = getValue<string>();

                return (
                    <CellWithCopy value={collection}>
                        <a href={appUrl.forDocuments(collection, databaseName)}>{collection}</a>
                    </CellWithCopy>
                );
            },
            size: sizeProvider(15),
        },
        {
            accessorKey: "Etag",
            cell: CellWithCopyWrapper,
            size: sizeProvider(10),
        },
        {
            header: "Change Vector",
            accessorKey: "ChangeVector",
            cell: CellWithCopyWrapper,
            size: sizeProvider(isSharded ? 20 : 25),
        },
        {
            header: "Last Modified",
            accessorKey: "LastModified",
            cell: CellWithCopyWrapper,
            size: sizeProvider(20),
        },
        {
            id: "Flags",
            header: () => (
                <span>
                    <Icon icon="flag" />
                    Flags
                </span>
            ),
            accessorKey: "Flags",
            cell: ({ getValue }) => {
                const flags = getValue<string>();

                return (
                    <span className="flags">
                        <Icon
                            icon="attachment"
                            title="Attachments"
                            className={classNames({ attachments: flags.includes("HasAttachments") })}
                        />
                        <Icon
                            icon="new-counter"
                            title="Counters"
                            className={classNames({ counters: flags.includes("HasCounters") })}
                        />
                        <Icon
                            icon="new-time-series"
                            title="Time Series"
                            className={classNames({ "time-series": flags.includes("HasTimeSeries") })}
                        />
                        <Icon
                            icon="data-archival"
                            title="Archived"
                            className={classNames({ archived: flags.includes("Archived") })}
                        />
                        <Icon
                            icon="trash"
                            title="Deleted Revision"
                            className={classNames({ "deleted-revision": flags.includes("DeleteRevision") })}
                        />
                    </span>
                );
            },
            size: sizeProvider(10),
        },
    ];

    if (isSharded) {
        columns.push({
            header: () => <Icon icon="shard" margin="m-0" title="Shard number" />,
            accessorKey: "ShardNumber",
            cell: CellWithCopyWrapper,
            size: sizeProvider(5),
        });
    }

    return columns;
};

const checkboxWidth = 38;

export const allRevisionsUtils = {
    getColumnDefs,
    smallSampleSize: 100, // we only want some preview
};
