import { CustomDropdownToggle } from "components/common/Dropdown";
import { Icon } from "components/common/Icon";
import {
    editCdcSinkTaskActions,
    editCdcSinkTaskSelectors,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import {
    castToEmbeddedTablePath,
    castToLinkedTablePath,
    EmbeddedTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskFormPaths";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useEditCdcSinkTaskTableActions } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskTableActions";
import { useAppDispatch, useAppSelector } from "components/store";
import classNames from "classnames";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import { useFormContext, useWatch } from "react-hook-form";
import { EditCdcSinkTaskLinkedTableItem } from "./EditCdcSinkTaskLinkedTableItem";
import { useErrorMessage } from "components/common/Form";

interface EditCdcSinkTaskEmbeddedTableItemProps {
    path: EmbeddedTablePath;
    isRootDisabled: boolean;
}

export function EditCdcSinkTaskEmbeddedTableItem({ path, isRootDisabled }: EditCdcSinkTaskEmbeddedTableItemProps) {
    const dispatch = useAppDispatch();
    const tableActions = useEditCdcSinkTaskTableActions();
    const expandedTables = useAppSelector(editCdcSinkTaskSelectors.expandedTables);
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);

    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const formError = useErrorMessage({ control, paths: [path] });
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
            <div className="hstack">
                <Button
                    variant={isActive ? "secondary" : "link"}
                    className={classNames("text-body text-start hstack flex-grow-1 overflow-hidden", {
                        "opacity-50": isRootDisabled,
                    })}
                    onClick={handleClick}
                    title={label}
                    style={{ paddingInline: "2px", minWidth: 0 }}
                >
                    <Icon
                        icon={isExpanded ? "chevron-down" : "chevron-right"}
                        className={classNames("font-size-12", { "opacity-0": !hasChildren })}
                        margin="m-0"
                    />
                    <span className="text-truncate" style={{ marginLeft: "2px" }}>
                        {label}
                    </span>
                    <Icon icon="embed" margin="ms-1" className="font-size-14" />
                    {formError.hasErrors && (
                        <Icon icon="warning" color="danger" className="font-size-14" margin="ms-1" />
                    )}
                </Button>
                <Dropdown>
                    <Dropdown.Toggle
                        as={CustomDropdownToggle}
                        isCaretHidden
                        variant="link"
                        className="text-body p-1"
                        title="Table actions"
                        size="xs"
                    >
                        <Icon icon="more" margin="m-0" />
                    </Dropdown.Toggle>
                    <Dropdown.Menu renderOnMount popperConfig={{ strategy: "fixed" }}>
                        <Dropdown.Item onClick={() => tableActions.addEmbeddedTable(path)}>
                            <Icon icon="embed" /> Add new embedded table
                        </Dropdown.Item>
                        <Dropdown.Item onClick={() => tableActions.addLinkedTable(path)}>
                            <Icon icon="link" /> Add new linked table
                        </Dropdown.Item>
                        <Dropdown.Item onClick={() => tableActions.changeEmbeddedToLinked(path)}>
                            <Icon icon="link" /> Change to linked
                        </Dropdown.Item>
                        <Dropdown.Item
                            className="text-danger"
                            onClick={() => tableActions.removeTable({ path, type: "embedded" })}
                        >
                            <Icon icon="trash" /> Remove
                        </Dropdown.Item>
                    </Dropdown.Menu>
                </Dropdown>
            </div>
            {hasChildren && isExpanded && (
                <div
                    className="vstack gap-1 border-start border-secondary ps-1 flex-grow-0"
                    style={{ marginLeft: "9px" }}
                >
                    {linkedTables.map((_, idx) => (
                        <EditCdcSinkTaskLinkedTableItem
                            key={idx}
                            path={castToLinkedTablePath(`${path}.linkedTables.${idx}`)}
                            isRootDisabled={isRootDisabled}
                        />
                    ))}
                    {embeddedTables.map((_, idx) => (
                        <EditCdcSinkTaskEmbeddedTableItem
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
