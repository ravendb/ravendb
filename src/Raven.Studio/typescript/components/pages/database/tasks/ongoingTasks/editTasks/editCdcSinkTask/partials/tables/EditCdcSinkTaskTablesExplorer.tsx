import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { useMemo, useState } from "react";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { UseFieldArrayReturn } from "react-hook-form";
import { useAppDispatch, useAppSelector } from "components/store";
import {
    editCdcSinkTaskActions,
    editCdcSinkTaskSelectors,
    FormTableInfo,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import classNames from "classnames";

type FormTablePath = string;
type FormTable = NonNullable<EditCdcSinkTaskFormData["tables"]>[number];
type FormEmbeddedTable = NonNullable<FormTable["embeddedTables"]>[number];
type FormLinkedTable = NonNullable<FormTable["linkedTables"]>[number];
type FormTableItem = FormTable | FormEmbeddedTable | FormLinkedTable;

interface EditCdcSinkTaskTablesExplorerProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskTablesExplorer({ tablesFieldArray }: EditCdcSinkTaskTablesExplorerProps) {
    const dispatch = useAppDispatch();
    const [filter, setFilter] = useState("");

    const handleCollapseAll = () => {
        const newExpandedState = Object.fromEntries(
            getExpandableTablePaths(tablesFieldArray.fields).map((path) => [path, false])
        );
        dispatch(editCdcSinkTaskActions.tableExpandedSet(newExpandedState));
    };

    const handleExpandAll = () => {
        const newExpandedState = Object.fromEntries(
            getExpandableTablePaths(tablesFieldArray.fields).map((path) => [path, true])
        );
        dispatch(editCdcSinkTaskActions.tableExpandedSet(newExpandedState));
    };

    return (
        <div className="vstack gap-2 h-100">
            <div className="hstack">
                <div className="me-auto">Tables</div>
                <Button variant="link" size="xs" className="text-body" title="Add new root table">
                    <Icon icon="plus" margin="m-0" />
                </Button>
                <Button
                    variant="link"
                    size="xs"
                    className="text-body"
                    title="Collapse all tables"
                    onClick={handleCollapseAll}
                >
                    <Icon icon="collapse-vertical" margin="m-0" />
                </Button>
                <Button
                    variant="link"
                    size="xs"
                    className="text-body"
                    title="Expand all tables"
                    onClick={handleExpandAll}
                >
                    <Icon icon="expand-vertical" margin="m-0" />
                </Button>
            </div>
            <Form.Control
                type="text"
                size="sm"
                placeholder="Filter tables"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
            />
            <TableItems tablesFieldArray={tablesFieldArray} filter={filter} />
        </div>
    );
}

interface TableItemsProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
    filter: string;
}

function TableItems({ tablesFieldArray, filter }: TableItemsProps) {
    const formTables = useMemo(
        () => tablesFieldArray.fields.map((table, idx) => ({ table, formIdx: idx })),
        [tablesFieldArray.fields]
    );

    const filteredTables = useMemo(() => {
        if (!filter) {
            return formTables;
        }

        return formTables.filter(({ table }) => table.sourceTableName.toLowerCase().includes(filter.toLowerCase()));
    }, [filter, formTables]);

    if (formTables.length === 0) {
        return <EmptySet compact>Use the Schema Explorer to discover existing tables or add new manually.</EmptySet>;
    }

    if (filteredTables.length === 0) {
        return <EmptySet compact>No tables match the filter.</EmptySet>;
    }

    return (
        <div className="vstack gap-1 overflow-y-auto flex-grow-0">
            {filteredTables.map(({ table, formIdx }) => (
                <TableItem key={table.id} table={table} path={getRootTablePath(formIdx)} depth={0} type="root" />
            ))}
        </div>
    );
}

interface TableItemProps {
    table: FormTableItem;
    path: FormTablePath;
    depth: number;
    type: "root" | "linked" | "embedded";
    parents?: FormTableInfo[];
}

function TableItem({ table, path, depth = 0, type = "root", parents = [] }: TableItemProps) {
    const dispatch = useAppDispatch();
    const expandedTables = useAppSelector(editCdcSinkTaskSelectors.expandedTables);

    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);

    const hasChildren = hasChildTables(table);
    const label = getTableLabel(table);
    const isExpanded = expandedTables[path];

    const isActive = activeTable?.current.path === path;

    const parentsWithCurrent: FormTableInfo[] = [...parents, { path, type, label }];

    const handleClick = () => {
        dispatch(editCdcSinkTaskActions.activeTableSet({ parents, current: { path, type, label } }));
        dispatch(editCdcSinkTaskActions.tableExpandedOneToggled(path));
    };

    return (
        <div className="vstack gap-1">
            <Button
                variant={isActive ? "secondary" : "link"}
                className="text-body text-start hstack"
                onClick={handleClick}
                title={label}
                style={{ paddingInline: "2px" }}
            >
                <Icon
                    icon={isExpanded ? "chevron-down" : "chevron-right"}
                    className={classNames("font-size-12", { "opacity-0": !hasChildren })}
                    margin="m-0"
                    style={{ paddingTop: "4px" }}
                />
                <span className="text-truncate" style={{ maxWidth: "200px", marginLeft: "2px" }}>
                    {label}
                </span>
                {type === "linked" && <Icon icon="link" margin="ms-1" className="font-size-14" />}
                {type === "embedded" && <Icon icon="embed" margin="ms-1" className="font-size-14" />}
            </Button>
            {hasChildren && isExpanded && (
                <div
                    className="vstack gap-1 border-start border-secondary ps-1 flex-grow-0"
                    style={{ marginLeft: (depth + 1) * 9 }}
                >
                    {getLinkedTables(table).map((linked, idx) => (
                        <TableItem
                            key={`${path}.linkedTables.${idx}`}
                            type="linked"
                            table={linked}
                            depth={depth + 1}
                            parents={parentsWithCurrent}
                            path={`${path}.linkedTables.${idx}`}
                        />
                    ))}
                    {getEmbeddedTables(table).map((embedded, idx) => (
                        <TableItem
                            key={`${path}.embeddedTables.${idx}`}
                            type="embedded"
                            table={embedded}
                            depth={depth + 1}
                            parents={parentsWithCurrent}
                            path={`${path}.embeddedTables.${idx}`}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}

function getRootTablePath(idx: number): FormTablePath {
    return `tables.${idx}`;
}

function getExpandableTablePaths(tables: EditCdcSinkTaskFormData["tables"]): FormTablePath[] {
    return (tables ?? []).flatMap((table, idx) => getExpandableTablePathsForTable(table, getRootTablePath(idx)));
}

function getExpandableTablePathsForTable(table: FormTableItem, path: FormTablePath): FormTablePath[] {
    const childPaths = getEmbeddedTables(table).flatMap((embedded, idx) =>
        getExpandableTablePathsForTable(embedded, `${path}.embeddedTables.${idx}`)
    );

    return hasChildTables(table) ? [path, ...childPaths] : childPaths;
}

function getTableLabel(table: FormTableItem) {
    if (!table.sourceTableSchema) {
        return table.sourceTableName || "Unassigned table";
    }

    return `${table.sourceTableSchema}.${table.sourceTableName || "Unassigned table"}`;
}

function hasChildTables(table: FormTableItem) {
    return getLinkedTables(table).length > 0 || getEmbeddedTables(table).length > 0;
}

function getLinkedTables(table: FormTableItem): FormLinkedTable[] {
    return "linkedTables" in table ? (table.linkedTables ?? []) : [];
}

function getEmbeddedTables(table: FormTableItem): FormEmbeddedTable[] {
    return "embeddedTables" in table ? (table.embeddedTables ?? []) : [];
}
