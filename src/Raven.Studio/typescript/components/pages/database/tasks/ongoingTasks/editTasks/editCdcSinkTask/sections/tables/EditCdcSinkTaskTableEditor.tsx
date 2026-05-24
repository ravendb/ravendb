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
import Breadcrumb from "react-bootstrap/Breadcrumb";
import EditCdcSinkTaskRootTableEditor from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/tableEditor/EditCdcSinkTaskRootTableEditor";
import EditCdcSinkTaskLinkedTableEditor from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/tableEditor/EditCdcSinkTaskLinkedTableEditor";
import EditCdcSinkTaskEmbeddedTableEditor from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/tableEditor/EditCdcSinkTaskEmbeddedTableEditor";
import useEditCdcSinkTaskBreadcrumbs, {
    EditCdcSinkTaskBreadcrumbItem,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskBreadcrumbs";
import { castToRootTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { getShell } from "common/shell/shellAccessor";
import classNames from "classnames";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import EditCdcSinkTaskTestPanel from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/tableEditor/EditCdcSinkTaskTestPanel";
import { useEffect } from "react";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

export default function EditCdcSinkTaskTableEditor() {
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);

    if (!activeTable) {
        return <EmptySet className="text-muted">Select a table to view its configuration details.</EmptySet>;
    }

    return <ActiveTableEditor key={activeTable.path} activeTable={activeTable} />;
}

function ActiveTableEditor({ activeTable }: { activeTable: CdcActiveTable }) {
    const dispatch = useAppDispatch();
    const editForm = useFormContext<EditCdcSinkTaskFormData>();
    const breadcrumbItems = useEditCdcSinkTaskBreadcrumbs(activeTable.path);

    const pathParts = activeTable.path.split(".");
    const rootTablePath = castToRootTablePath(`${pathParts[0]}.${pathParts[1]}`);

    const isRootTableDisabled = useWatch({
        control: editForm.control,
        name: `${rootTablePath}.disabled`,
    });

    const viewSheet = useViewSheet();

    const handleOpenRootTableTest = () => {
        getShell()?.collapseMenu(true);
        viewSheet.open({
            component: <EditCdcSinkTaskTestPanel editForm={editForm} path={rootTablePath} />,
            isPinned: true,
            initialWidth: 375,
            minWidth: 300,
        });
    };

    // If test is open, automatically switch panel with current root table
    useEffect(() => {
        if (viewSheet.isOpen && activeTable.type === "root") {
            handleOpenRootTableTest();
        }
    }, [viewSheet.isOpen, activeTable.type, rootTablePath]);

    const handleBreadcrumbClick = (item: EditCdcSinkTaskBreadcrumbItem) => {
        if (item.isActive) {
            return;
        }
        dispatch(editCdcSinkTaskSlice.actions.activeTableSet(item));
    };

    return (
        <div className="cdc-table-editor h-100 vstack min-height-0">
            <div className="hstack gap-2 p-2 border-bottom border-secondary flex-wrap">
                <div className="flex-grow-1 overflow-hidden min-width-0">
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
                        <PopoverWithHoverWrapper message="Preview documents generated from sample rows.">
                            <Button variant="info" disabled={isRootTableDisabled} onClick={handleOpenRootTableTest}>
                                <Icon icon="rocket" />
                                Test
                            </Button>
                        </PopoverWithHoverWrapper>
                    )}
                </div>
            </div>
            <div
                className={classNames("p-2 flex-grow-1 overflow-y-auto min-height-0", {
                    "form-disabled": isRootTableDisabled,
                })}
            >
                {activeTable.type === "root" && <EditCdcSinkTaskRootTableEditor path={activeTable.path} />}
                {activeTable.type === "linked" && <EditCdcSinkTaskLinkedTableEditor path={activeTable.path} />}
                {activeTable.type === "embedded" && <EditCdcSinkTaskEmbeddedTableEditor path={activeTable.path} />}
            </div>
        </div>
    );
}
