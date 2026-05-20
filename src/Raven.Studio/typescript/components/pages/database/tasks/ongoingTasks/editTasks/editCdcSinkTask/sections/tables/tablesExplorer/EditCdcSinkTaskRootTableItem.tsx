import { CustomDropdownToggle, DropdownPortalMenu, DropdownWithPortalMenu } from "components/common/Dropdown";
import { Icon } from "components/common/Icon";
import {
    editCdcSinkTaskActions,
    editCdcSinkTaskSelectors,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { ExplorerRowRootTable } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useEditCdcSinkTaskTableActions } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskTableActions";
import { useAppDispatch, useAppSelector } from "components/store";
import classNames from "classnames";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import { useFormContext } from "react-hook-form";
import { FormErrorIcon } from "components/common/Form";
import { editCdcSinkTaskConstants } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskConstants";

const { expandButtonWidthPx } = editCdcSinkTaskConstants;

export function EditCdcSinkTaskRootTableItem({ path, table, isExpanded, hasChildren }: ExplorerRowRootTable) {
    const dispatch = useAppDispatch();
    const tableActions = useEditCdcSinkTaskTableActions();
    const isActive = useAppSelector(editCdcSinkTaskSelectors.isActiveTable(path));
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const label = table.sourceTableName || "Unassigned table";
    const isDisabled = table.disabled;

    const handleClick = () => {
        if (!isActive) {
            dispatch(editCdcSinkTaskActions.activeTableSet({ path, type: "root" }));
        }
    };

    const handleToggleExpanded = () => {
        if (hasChildren) {
            dispatch(editCdcSinkTaskActions.tableExpandedOneToggled(path));
        }
    };

    return (
        <div className="hstack">
            {hasChildren && (
                <Button
                    variant="link"
                    className="text-body p-0"
                    onClick={handleToggleExpanded}
                    title={isExpanded ? "Collapse table" : "Expand table"}
                    style={{ width: expandButtonWidthPx, minWidth: expandButtonWidthPx }}
                >
                    <Icon icon={isExpanded ? "chevron-down" : "chevron-right"} className="font-size-12" margin="m-0" />
                </Button>
            )}
            <Button
                variant={isActive ? "secondary" : "link"}
                className={classNames("text-body text-start hstack flex-grow-1 overflow-hidden", {
                    "opacity-50": isDisabled,
                })}
                onClick={handleClick}
                title={label}
                style={{ paddingInline: "2px", minWidth: 0, marginLeft: hasChildren ? undefined : expandButtonWidthPx }}
            >
                <span className="text-truncate" style={{ marginLeft: "2px" }}>
                    {label}
                </span>
                <FormErrorIcon control={control} paths={[path]} iconClassName="font-size-14" />
            </Button>
            <DropdownWithPortalMenu>
                <Dropdown.Toggle
                    as={CustomDropdownToggle}
                    isCaretHidden
                    variant="link"
                    className="text-body px-1"
                    title="Table actions"
                    size="xs"
                >
                    <Icon icon="more" margin="m-0" />
                </Dropdown.Toggle>
                <DropdownPortalMenu>
                    <Dropdown.Item onClick={() => tableActions.addEmbeddedTable(path)}>
                        <Icon icon="embed" /> Add new embedded table
                    </Dropdown.Item>
                    <Dropdown.Item onClick={() => tableActions.addLinkedTable(path)}>
                        <Icon icon="link" /> Add new linked table
                    </Dropdown.Item>
                    <Dropdown.Item onClick={() => tableActions.toggleRootTableDisabled(path)}>
                        <Icon icon={isDisabled ? "play" : "stop"} /> {isDisabled ? "Enable" : "Disable"}
                    </Dropdown.Item>
                    <Dropdown.Item
                        className="text-danger"
                        onClick={() => tableActions.removeTable({ path, type: "root" })}
                    >
                        <Icon icon="trash" /> Remove
                    </Dropdown.Item>
                </DropdownPortalMenu>
            </DropdownWithPortalMenu>
        </div>
    );
}
