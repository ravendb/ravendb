import CollapseButton from "components/common/CollapseButton";
import useBoolean from "components/hooks/useBoolean";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import Collapse from "react-bootstrap/esm/Collapse";
import { UseFieldArrayReturn } from "react-hook-form";

interface EditCdcSinkTaskTablesProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskTables({ tablesFieldArray }: EditCdcSinkTaskTablesProps) {
    const { value: isPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    return (
        <div className="mt-3">
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

function TablesPanel({
    tablesFieldArray,
}: {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}) {
    return (
        <div>
            {tablesFieldArray.fields.map((field, index) => (
                <div key={field.id}>{field.CollectionName}</div>
            ))}
        </div>
    );
}
