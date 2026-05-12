import { EmptySet } from "components/common/EmptySet";
import {
    CdcActiveTable,
    editCdcSinkTaskSelectors,
    editCdcSinkTaskSlice,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppDispatch, useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { FormSwitch } from "components/common/Form";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import { useEditCdcSinkTaskTableActions } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskTableActions";
import EditCdcSinkTaskRootTableEditor from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/tableEditor/EditCdcSinkTaskRootTableEditor";
import EditCdcSinkTaskLinkedTableEditor from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/tableEditor/EditCdcSinkTaskLinkedTableEditor";
import EditCdcSinkTaskEmbeddedTableEditor from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/tableEditor/EditCdcSinkTaskEmbeddedTableEditor";
import useEditCdcSinkTaskBreadcrumbs, {
    EditCdcSinkTaskBreadcrumbItem,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskBreadcrumbs";
import { castToRootTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import classNames from "classnames";

export default function EditCdcSinkTaskTableEditor() {
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);

    if (!activeTable) {
        return <EmptySet>Select a table to view its configuration details.</EmptySet>;
    }

    return <ActiveTableEditor key={activeTable.path} activeTable={activeTable} />;
}

function ActiveTableEditor({ activeTable }: { activeTable: CdcActiveTable }) {
    const dispatch = useAppDispatch();
    const tableActions = useEditCdcSinkTaskTableActions();
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const breadcrumbItems = useEditCdcSinkTaskBreadcrumbs(activeTable.path);

    const pathParts = activeTable.path.split(".");
    const rootTablePath = castToRootTablePath(`${pathParts[0]}.${pathParts[1]}`);

    const isRootTableDisabled = useWatch({
        control,
        name: `${rootTablePath}.disabled`,
    });

    const handleBreadcrumbClick = (item: EditCdcSinkTaskBreadcrumbItem) => {
        if (item.isActive) {
            return;
        }
        dispatch(editCdcSinkTaskSlice.actions.activeTableSet(item));
    };

    return (
        <div className="cdc-table-editor h-100 vstack">
            <div className="hstack gap-2 p-2 border-bottom border-secondary flex-wrap">
                <div className="flex-grow-1 overflow-hidden" style={{ minWidth: 0 }}>
                    <Breadcrumb className="mb-0">
                        {breadcrumbItems.map((item) => (
                            <Breadcrumb.Item
                                key={item.path}
                                active={item.isActive}
                                onClick={() => handleBreadcrumbClick(item)}
                            >
                                {item.label}
                            </Breadcrumb.Item>
                        ))}
                    </Breadcrumb>
                </div>
                <div className="ms-auto hstack gap-2 align-items-center">
                    {activeTable.type === "root" && (
                        <FormSwitch control={control} name={`${activeTable.path}.disabled`}>
                            Disabled
                        </FormSwitch>
                    )}
                    {activeTable.type === "linked" && (
                        <Button
                            variant="secondary"
                            onClick={() => tableActions.changeLinkedToEmbedded(activeTable.path)}
                        >
                            <Icon icon="embed" />
                            Change to embedded
                        </Button>
                    )}
                    {activeTable.type === "embedded" && (
                        <Button
                            variant="secondary"
                            onClick={() => tableActions.changeEmbeddedToLinked(activeTable.path)}
                        >
                            <Icon icon="link" />
                            Change to linked
                        </Button>
                    )}
                    <Button variant="info">
                        <Icon icon="rocket" />
                        Test
                    </Button>
                </div>
            </div>
            <div className={classNames("p-2 flex-grow-1 overflow-y-auto", { "form-disabled": isRootTableDisabled })}>
                {activeTable.type === "root" && <EditCdcSinkTaskRootTableEditor path={activeTable.path} />}
                {activeTable.type === "linked" && <EditCdcSinkTaskLinkedTableEditor path={activeTable.path} />}
                {activeTable.type === "embedded" && <EditCdcSinkTaskEmbeddedTableEditor path={activeTable.path} />}
            </div>
        </div>
    );
}
