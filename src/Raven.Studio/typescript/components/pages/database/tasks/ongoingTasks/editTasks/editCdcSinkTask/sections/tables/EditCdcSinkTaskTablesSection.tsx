import CollapseButton from "components/common/CollapseButton";
import useBoolean from "components/hooks/useBoolean";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import Collapse from "react-bootstrap/esm/Collapse";
import { UseFieldArrayReturn, useFormContext } from "react-hook-form";
import EditCdcSinkTaskTablesExplorer from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/EditCdcSinkTaskTablesExplorer";
import EditCdcSinkTaskTableEditor from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/EditCdcSinkTaskTableEditor";
import useResizableWidth from "components/hooks/useResizableWidth";
import ColumnResize from "components/common/ColumnResize";
import classNames from "classnames";
import { FormErrorIcon } from "components/common/Form";

interface EditCdcSinkTaskTablesSectionProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskTablesSection({ tablesFieldArray }: EditCdcSinkTaskTablesSectionProps) {
    const { value: isPanelOpen, toggle: togglePanel, setTrue: openPanel } = useBoolean(true);
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    return (
        <div className="mt-3 vstack pb-3" style={{ minHeight: tablesFieldArray.fields.length > 0 ? "100%" : "300px" }}>
            <div className="hstack align-items-center">
                <h3 className="m-0">Configured Tables</h3>
                <FormErrorIcon control={control} paths={["tables"]} onError={openPanel} />
                <CollapseButton isExpanded={isPanelOpen} toggle={togglePanel} />
            </div>
            <div className="mb-1">Customize the source tables or add new ones.</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <TablesPanel tablesFieldArray={tablesFieldArray} />
            </Collapse>
        </div>
    );
}

function TablesPanel({ tablesFieldArray }: EditCdcSinkTaskTablesSectionProps) {
    const resizable = useResizableWidth({
        initialWidth: 300,
        minWidth: 190,
        maxWidth: 500,
        placement: "right",
    });

    return (
        <div className="mt-3 hstack align-items-stretch panel-bg-2 rounded-2 border border-secondary flex-grow-1 min-height-0">
            <div
                className={classNames("rounded-2 h-100 p-2 position-relative", {
                    "is-dragging": resizable.isDragging,
                })}
                style={{ width: resizable.width }}
            >
                <EditCdcSinkTaskTablesExplorer tablesFieldArray={tablesFieldArray} />
                <ColumnResize handleMouseDown={resizable.handleMouseDown} placement="right" />
            </div>
            <div className="border-start border-secondary panel-bg-1 rounded-end-2 flex-grow-1 overflow-hidden min-height-0">
                <EditCdcSinkTaskTableEditor />
            </div>
        </div>
    );
}
