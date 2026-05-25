import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import Form from "react-bootstrap/Form";
import { Icon } from "components/common/Icon";
import { useMemo, useState } from "react";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { FieldPath, UseFieldArrayReturn } from "react-hook-form";
import { useAppDispatch, useAppSelector } from "components/store";
import {
    editCdcSinkTaskActions,
    editCdcSinkTaskSelectors,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { getRootTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskTableItems } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/tablesExplorer/EditCdcSinkTaskTableItems";
import { Switch } from "components/common/Checkbox";
import { CustomDropdownToggle } from "components/common/Dropdown";

interface EditCdcSinkTaskTablesExplorerProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskTablesExplorer({ tablesFieldArray }: EditCdcSinkTaskTablesExplorerProps) {
    const dispatch = useAppDispatch();
    const isFieldMappingExpandedByDefault = useAppSelector(editCdcSinkTaskSelectors.isFieldMappingExpandedByDefault);
    const [filter, setFilter] = useState("");
    const rootFieldIds = useMemo(() => tablesFieldArray.fields.map((field) => field.id), [tablesFieldArray.fields]);

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
                <Dropdown>
                    <Dropdown.Toggle
                        as={CustomDropdownToggle}
                        isCaretHidden
                        variant="link"
                        className="text-body"
                        title="Table settings"
                        size="xs"
                    >
                        <Icon icon="settings" margin="m-0" />
                    </Dropdown.Toggle>
                    <Dropdown.Menu>
                        <Dropdown.ItemText className="text-nowrap">
                            <Switch
                                color="primary"
                                selected={isFieldMappingExpandedByDefault}
                                toggleSelection={(e) =>
                                    dispatch(
                                        editCdcSinkTaskActions.fieldMappingExpandedByDefaultSet(e.currentTarget.checked)
                                    )
                                }
                            >
                                Expand field mapping
                            </Switch>
                        </Dropdown.ItemText>
                    </Dropdown.Menu>
                </Dropdown>
            </div>
            <Form.Control
                type="text"
                size="sm"
                placeholder="Filter tables"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
            />
            <EditCdcSinkTaskTableItems filter={filter} rootFieldIds={rootFieldIds} />
        </div>
    );
}
