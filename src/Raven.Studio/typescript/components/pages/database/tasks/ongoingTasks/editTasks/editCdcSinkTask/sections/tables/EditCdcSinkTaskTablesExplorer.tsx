import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { useMemo, useState } from "react";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { FieldPath, UseFieldArrayReturn, useFormContext, useWatch } from "react-hook-form";
import { useAppDispatch, useAppSelector } from "components/store";
import {
    editCdcSinkTaskActions,
    editCdcSinkTaskSelectors,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import classNames from "classnames";
import {
    getRootTablePath,
    castToLinkedTablePath,
    castToEmbeddedTablePath,
    EmbeddedTablePath,
    LinkedTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskFormPaths";
import _ from "lodash";

interface EditCdcSinkTaskTablesExplorerProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskTablesExplorer({ tablesFieldArray }: EditCdcSinkTaskTablesExplorerProps) {
    const dispatch = useAppDispatch();
    const [filter, setFilter] = useState("");

    const handleAddRootTable = () => {
        tablesFieldArray.append({
            sourceTableName: "",
            sourceTableSchema: "public",
            collectionName: "NewCollection",
            columns: [],
            primaryKeyColumns: [],
            linkedTables: [],
            embeddedTables: [],
            onDelete: {
                ignoreDeletes: false,
                patch: "",
            },
            disabled: false,
            patch: "",
        });

        dispatch(
            editCdcSinkTaskActions.activeTableSet({
                type: "root",
                path: getRootTablePath(tablesFieldArray.fields.length),
            })
        );
    };

    const handleExpandedSet = (isExpanded: boolean) => {
        const expanded: Partial<Record<FieldPath<EditCdcSinkTaskFormData>, boolean>> = {};
        tablesFieldArray.fields.forEach((_, idx) => {
            const path = getRootTablePath(idx);
            expanded[path] = isExpanded;
        });

        dispatch(editCdcSinkTaskActions.tableExpandedSet(expanded));
    };

    return (
        <div className="vstack gap-2 h-100">
            <div className="hstack">
                <div className="me-auto">Tables</div>
                <Button
                    variant="link"
                    size="xs"
                    className="text-body"
                    title="Add new root table"
                    onClick={handleAddRootTable}
                >
                    <Icon icon="plus" margin="m-0" />
                </Button>
                <Button
                    variant="link"
                    size="xs"
                    className="text-body"
                    title="Collapse all tables"
                    onClick={() => handleExpandedSet(false)}
                >
                    <Icon icon="collapse-vertical" margin="m-0" />
                </Button>
                <Button
                    variant="link"
                    size="xs"
                    className="text-body"
                    title="Expand all tables"
                    onClick={() => handleExpandedSet(true)}
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
    const filteredGroupedTables = useMemo(() => {
        const normalizedFilter = filter.trim().toLowerCase();

        return Object.entries(_.groupBy(tablesFieldArray.fields, (table) => table.sourceTableSchema || "default"))
            .map(([schema, tables]) => ({
                schema,
                tables: normalizedFilter
                    ? tables.filter((table) => table.sourceTableName.toLowerCase().includes(normalizedFilter))
                    : tables,
            }))
            .filter((group) => group.tables.length > 0);
    }, [filter, tablesFieldArray.fields]);

    if (tablesFieldArray.fields.length === 0) {
        return <EmptySet compact>Use the Schema Explorer to discover existing tables or add new manually.</EmptySet>;
    }

    if (filteredGroupedTables.length === 0) {
        return <EmptySet compact>No tables match the filter.</EmptySet>;
    }

    return (
        <div className="vstack gap-1 overflow-y-auto flex-grow-0">
            {filteredGroupedTables.map(({ schema, tables }) => (
                <div key={schema} className="vstack gap-1">
                    <div className="text-center font-monospace small">{schema}</div>
                    {tables.map((table) => (
                        <RootTableItem
                            key={table.id}
                            formIdx={tablesFieldArray.fields.findIndex((t) => t.id === table.id)!}
                        />
                    ))}
                </div>
            ))}
        </div>
    );
}

interface RootTableItemProps {
    formIdx: number;
}

function RootTableItem({ formIdx }: RootTableItemProps) {
    const dispatch = useAppDispatch();
    const expandedTables = useAppSelector(editCdcSinkTaskSelectors.expandedTables);
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);

    const path = getRootTablePath(formIdx);

    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const isDisabled = useWatch({ control, name: `${path}.disabled` });
    const linkedTables = useWatch({ control, name: `${path}.linkedTables` });
    const embeddedTables = useWatch({ control, name: `${path}.embeddedTables` });
    const sourceTableName = useWatch({ control, name: `${path}.sourceTableName` });

    const hasChildren = embeddedTables.length > 0 || linkedTables.length > 0;
    const label = sourceTableName || "Unassigned table";
    const isExpanded = expandedTables[path];

    const isActive = activeTable?.path === path;

    const handleClick = () => {
        dispatch(editCdcSinkTaskActions.activeTableSet({ path, type: "root" }));
        dispatch(editCdcSinkTaskActions.tableExpandedOneToggled(path));
    };

    return (
        <div className="vstack gap-1">
            <Button
                variant={isActive ? "secondary" : "link"}
                className={classNames("text-body text-start hstack", { "opacity-50": isDisabled })}
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
            </Button>
            {hasChildren && isExpanded && (
                <div
                    className="vstack gap-1 border-start border-secondary ps-1 flex-grow-0"
                    style={{ marginLeft: "9px" }}
                >
                    {linkedTables.map((_, idx) => (
                        <LinkedTableItem key={idx} path={`${path}.linkedTables.${idx}`} isRootDisabled={isDisabled} />
                    ))}
                    {embeddedTables.map((_, idx) => (
                        <EmbeddedTableItem
                            key={idx}
                            path={`${path}.embeddedTables.${idx}`}
                            isRootDisabled={isDisabled}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}

interface LinkedTableItemProps {
    path: LinkedTablePath;
    isRootDisabled: boolean;
}

function LinkedTableItem({ path, isRootDisabled }: LinkedTableItemProps) {
    const dispatch = useAppDispatch();
    const expandedTables = useAppSelector(editCdcSinkTaskSelectors.expandedTables);
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);

    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const sourceTableName = useWatch({ control, name: `${path}.sourceTableName` });

    const label = sourceTableName || "Unassigned table";
    const isExpanded = expandedTables[path];

    const isActive = activeTable?.path === path;

    const handleClick = () => {
        dispatch(editCdcSinkTaskActions.activeTableSet({ path, type: "linked" }));
        dispatch(editCdcSinkTaskActions.tableExpandedOneToggled(path));
    };

    return (
        <div className="vstack gap-1">
            <Button
                variant={isActive ? "secondary" : "link"}
                className={classNames("text-body text-start hstack", { "opacity-50": isRootDisabled })}
                onClick={handleClick}
                title={label}
                style={{ paddingInline: "2px" }}
            >
                <Icon
                    icon={isExpanded ? "chevron-down" : "chevron-right"}
                    className="font-size-12 opacity-0"
                    margin="m-0"
                    style={{ paddingTop: "4px" }}
                />
                <span className="text-truncate" style={{ maxWidth: "200px", marginLeft: "2px" }}>
                    {label}
                </span>
                <Icon icon="link" margin="ms-1" className="font-size-14" />
            </Button>
        </div>
    );
}

interface EmbeddedTableItemProps {
    path: EmbeddedTablePath;
    isRootDisabled: boolean;
}

function EmbeddedTableItem({ path, isRootDisabled }: EmbeddedTableItemProps) {
    const dispatch = useAppDispatch();
    const expandedTables = useAppSelector(editCdcSinkTaskSelectors.expandedTables);
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);

    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const embeddedTables = useWatch({ control, name: `${path}.embeddedTables` });
    const linkedTables = useWatch({ control, name: `${path}.linkedTables` });
    const sourceTableName = useWatch({ control, name: `${path}.sourceTableName` });

    const hasChildren = embeddedTables.length > 0 || linkedTables.length > 0;
    const label = sourceTableName || "Unassigned table";
    const isExpanded = expandedTables[path];

    const isActive = activeTable?.path === path;

    const handleClick = () => {
        dispatch(editCdcSinkTaskActions.activeTableSet({ path, type: "embedded" }));
        dispatch(editCdcSinkTaskActions.tableExpandedOneToggled(path));
    };

    return (
        <div className="vstack gap-1">
            <Button
                variant={isActive ? "secondary" : "link"}
                className={classNames("text-body text-start hstack", { "opacity-50": isRootDisabled })}
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
                <Icon icon="embed" margin="ms-1" className="font-size-14" />
            </Button>
            {hasChildren && isExpanded && (
                <div
                    className="vstack gap-1 border-start border-secondary ps-1 flex-grow-0"
                    style={{ marginLeft: "9px" }}
                >
                    {linkedTables.map((_, idx) => (
                        <LinkedTableItem
                            key={idx}
                            path={castToLinkedTablePath(`${path}.linkedTables.${idx}`)}
                            isRootDisabled={isRootDisabled}
                        />
                    ))}
                    {embeddedTables.map((_, idx) => (
                        <EmbeddedTableItem
                            key={idx}
                            path={castToEmbeddedTablePath(`${path}.embeddedTables.${idx}`)}
                            isRootDisabled={isRootDisabled}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}
