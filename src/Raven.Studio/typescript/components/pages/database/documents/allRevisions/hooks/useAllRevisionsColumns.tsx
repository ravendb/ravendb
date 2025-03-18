import { ColumnDef } from "@tanstack/react-table";
import classNames from "classnames";
import { RevisionsPreviewResultItem } from "commands/database/documents/getRevisionsPreviewCommand";
import appUrl from "common/appUrl";
import { Checkbox } from "components/common/Checkbox";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { Icon } from "components/common/Icon";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { CellWithCopy, CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";

export function useAllRevisionsColumns(
    databaseName: string,
    isSharded: boolean,
    tableBodyWidth: number,
    rowSelection: RevisionsPreviewResultItem[],
    setRowSelection: (rows: RevisionsPreviewResultItem[]) => void
): ColumnDef<RevisionsPreviewResultItem>[] {
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const checkboxWidth = hasDatabaseAdminAccess ? 38 : 0;

    const sizeProvider = virtualTableUtils.getCellSizeProvider(tableBodyWidth - checkboxWidth);

    const columns: ColumnDef<RevisionsPreviewResultItem>[] = [];

    if (hasDatabaseAdminAccess) {
        columns.push({
            id: "Checkbox",
            header: "",
            accessorFn: (x) => x,
            cell: ({ getValue }) => (
                <CheckboxCell
                    rowSelection={rowSelection}
                    setRowSelection={setRowSelection}
                    rowValue={getValue<RevisionsPreviewResultItem>()}
                />
            ),
            size: checkboxWidth,
            minSize: checkboxWidth,
            enableSorting: false,
            enableHiding: false,
        });
    }

    columns.push(
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
                            title='"HasAttachments"'
                            className={classNames({ attachments: flags.includes("HasAttachments") })}
                        />
                        <Icon
                            icon="new-counter"
                            title='"HasCounters"'
                            className={classNames({ counters: flags.includes("HasCounters") })}
                        />
                        <Icon
                            icon="new-time-series"
                            title='"HasTimeSeries"'
                            className={classNames({ "time-series": flags.includes("HasTimeSeries") })}
                        />
                        <Icon
                            icon="data-archival"
                            title='"Archived"'
                            className={classNames({ archived: flags.includes("Archived") })}
                        />
                        <Icon
                            icon="trash"
                            title='"DeleteRevision"'
                            className={classNames({ "deleted-revision": flags.includes("DeleteRevision") })}
                        />
                    </span>
                );
            },
            size: sizeProvider(10),
        }
    );

    if (isSharded) {
        columns.push({
            header: () => <Icon icon="shard" margin="m-0" title="Shard number" />,
            accessorKey: "ShardNumber",
            cell: CellWithCopyWrapper,
            size: sizeProvider(5),
        });
    }

    return columns;
}

interface CheckboxCellProps {
    rowValue: RevisionsPreviewResultItem;
    rowSelection: RevisionsPreviewResultItem[];
    setRowSelection: (rows: RevisionsPreviewResultItem[]) => void;
}

function CheckboxCell({ rowValue, rowSelection, setRowSelection }: CheckboxCellProps) {
    const { forCurrentDatabase } = useAppUrls();

    const isDeleteRevision = rowValue.Flags.includes("DeleteRevision");

    const isSelected = !!rowSelection.find((x) => isEqualRevision(x, rowValue));

    const toggleSelection = () => {
        if (isSelected) {
            setRowSelection(rowSelection.filter((x) => !isEqualRevision(x, rowValue)));
        } else {
            setRowSelection([...rowSelection, rowValue]);
        }
    };

    return (
        <ConditionalPopover
            conditions={{
                isActive: isDeleteRevision,
                message: (
                    <div className="text-center">
                        A &quot;Delete Revision&quot; can only be deleted from the
                        <br />
                        <a href={forCurrentDatabase.revisionsBin()} target="_blank">
                            Revisions Bin view
                        </a>
                    </div>
                ),
            }}
            popoverPlacement="top"
        >
            <Checkbox selected={isSelected} toggleSelection={toggleSelection} disabled={isDeleteRevision} />
        </ConditionalPopover>
    );
}

function isEqualRevision(a: RevisionsPreviewResultItem, b: RevisionsPreviewResultItem) {
    return a.Id === b.Id && a.ChangeVector === b.ChangeVector;
}
