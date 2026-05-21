import { CustomDropdownToggle, DropdownPortalMenu, DropdownWithPortalMenu } from "components/common/Dropdown";
import { Icon } from "components/common/Icon";
import {
    editCdcSinkTaskActions,
    editCdcSinkTaskSelectors,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { ExplorerRowLinkedTable } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useEditCdcSinkTaskTableActions } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskTableActions";
import { useAppDispatch, useAppSelector } from "components/store";
import classNames from "classnames";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import { useFormContext } from "react-hook-form";
import { FormErrorIcon } from "components/common/Form";
import { editCdcSinkTaskConstants } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskConstants";

const { expandButtonWidthPx, nestedTableIndentPx } = editCdcSinkTaskConstants;

export function EditCdcSinkTaskLinkedTableItem({ path, table, depth, isRootDisabled }: ExplorerRowLinkedTable) {
    const dispatch = useAppDispatch();
    const tableActions = useEditCdcSinkTaskTableActions();
    const isActive = useAppSelector(editCdcSinkTaskSelectors.isActiveTable(path));
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const label = table.sourceTableName || "Unassigned table";

    const handleClick = () => {
        if (!isActive) {
            dispatch(editCdcSinkTaskActions.activeTableSet({ path, type: "linked" }));
        }
    };

    return (
        <div className="hstack border-start border-secondary" style={{ marginLeft: depth * nestedTableIndentPx }}>
            <Button
                variant={isActive ? "secondary" : "link"}
                className={classNames("text-body text-start hstack flex-grow-1 overflow-hidden", {
                    "opacity-50": isRootDisabled,
                })}
                onClick={handleClick}
                title={label}
                style={{ paddingInline: "2px", minWidth: 0, marginLeft: expandButtonWidthPx }}
            >
                <span className="text-truncate" style={{ marginLeft: "2px" }}>
                    {label}
                </span>
                <Icon icon="link" margin="ms-1" className="font-size-14" />
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
                    <Dropdown.Item
                        className="text-danger"
                        onClick={() => tableActions.removeTable({ path, type: "linked" })}
                    >
                        <Icon icon="trash" /> Remove
                    </Dropdown.Item>
                </DropdownPortalMenu>
            </DropdownWithPortalMenu>
        </div>
    );
}
