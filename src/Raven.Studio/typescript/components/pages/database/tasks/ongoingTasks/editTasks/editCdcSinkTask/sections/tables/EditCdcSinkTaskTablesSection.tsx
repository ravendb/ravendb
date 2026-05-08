import CollapseButton from "components/common/CollapseButton";
import useBoolean from "components/hooks/useBoolean";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import Collapse from "react-bootstrap/esm/Collapse";
import { UseFieldArrayReturn } from "react-hook-form";
import EditCdcSinkTaskTablesExplorer from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/EditCdcSinkTaskTablesExplorer";
import EditCdcSinkTaskTableEditor from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/EditCdcSinkTaskTableEditor";

interface EditCdcSinkTaskTablesSectionProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskTablesSection({ tablesFieldArray }: EditCdcSinkTaskTablesSectionProps) {
    const { value: isPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    return (
        <div className="mt-3 vstack" style={{ minHeight: "100%" }}>
            <div className="hstack align-items-center">
                <h3 className="m-0">Configured Tables</h3>
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">Customize the source tables or add new ones.</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <TablesPanel tablesFieldArray={tablesFieldArray} />
            </Collapse>
        </div>
    );
}

function TablesPanel({ tablesFieldArray }: EditCdcSinkTaskTablesSectionProps) {
    return (
        <div className="mt-3 hstack align-items-start panel-bg-2 rounded-2 border border-secondary overflow-x-hidden flex-grow-1">
            <div className="rounded-2 overflow-y-auto h-100 p-2" style={{ minWidth: "300px", width: "300px" }}>
                <EditCdcSinkTaskTablesExplorer tablesFieldArray={tablesFieldArray} />
            </div>
            <div
                className="border-start border-secondary panel-bg-1 rounded-end-2 flex-grow-1"
                style={{ minHeight: "100%" }}
            >
                <EditCdcSinkTaskTableEditor />
            </div>
        </div>
    );
}
