import { CustomDropdownToggle } from "components/common/Dropdown";
import { Icon } from "components/common/Icon";
import {
    editCdcSinkTaskActions,
    editCdcSinkTaskSelectors,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { LinkedTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskFormPaths";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useEditCdcSinkTaskTableActions } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskTableActions";
import { useAppDispatch, useAppSelector } from "components/store";
import classNames from "classnames";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import { useFormContext, useWatch } from "react-hook-form";
import { useErrorMessage } from "components/common/Form";

interface EditCdcSinkTaskLinkedTableItemProps {
    path: LinkedTablePath;
    isRootDisabled: boolean;
}

export function EditCdcSinkTaskLinkedTableItem({ path, isRootDisabled }: EditCdcSinkTaskLinkedTableItemProps) {
    const dispatch = useAppDispatch();
    const tableActions = useEditCdcSinkTaskTableActions();
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);

    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const formError = useErrorMessage({ control, paths: [path] });
    const sourceTableName = useWatch({ control, name: `${path}.sourceTableName` });

    const label = sourceTableName || "Unassigned table";

    const isActive = activeTable?.path === path;

    const handleClick = () => {
        dispatch(editCdcSinkTaskActions.activeTableSet({ path, type: "linked" }));
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
                    <Icon icon="chevron-right" className="font-size-12 opacity-0" margin="m-0" />
                    <span className="text-truncate" style={{ marginLeft: "2px" }}>
                        {label}
                    </span>
                    <Icon icon="link" margin="ms-1" className="font-size-14" />
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
                        <Dropdown.Item onClick={() => tableActions.changeLinkedToEmbedded(path)}>
                            <Icon icon="embed" /> Change to embedded
                        </Dropdown.Item>
                        <Dropdown.Item
                            className="text-danger"
                            onClick={() => tableActions.removeTable({ path, type: "linked" })}
                        >
                            <Icon icon="trash" /> Remove
                        </Dropdown.Item>
                    </Dropdown.Menu>
                </Dropdown>
            </div>
        </div>
    );
}
