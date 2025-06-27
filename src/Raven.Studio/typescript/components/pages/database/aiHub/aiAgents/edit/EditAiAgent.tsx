import { AboutViewHeading } from "components/common/AboutView";
import useResizableWidth from "../hooks/useResizableWidth";
import { FormProvider, useForm } from "react-hook-form";
import { EditAiAgentFormData, editAiAgentYupResolver } from "./utils/editAiAgentValidation";
import EditAiAgentMain from "./EditAiAgentMain";
import EditAiAgentFooter from "./EditAiAgentFooter";
import EditAiAgentTestResults from "./EditAiAgentTestResults";

export default function EditAiAgent() {
    const form = useForm<EditAiAgentFormData>({
        resolver: editAiAgentYupResolver,
    });

    const {
        handleSubmit,
        formState: { errors },
    } = form;

    const testAreaResizable = useResizableWidth({
        initialWidth: 500,
        minWidth: 500,
        maxWidth: 1000,
    });

    const saveAgent = (data: EditAiAgentFormData) => {
        console.log(data);
    };

    console.log("kalczur error", errors);

    return (
        <FormProvider {...form}>
            <form onSubmit={handleSubmit(saveAgent)} className="h-100">
                <div className="hstack h-100">
                    <div className="vstack h-100">
                        <div className="p-3">
                            <AboutViewHeading title="Create AI Agent" icon="ai-agents" marginBottom={0} />
                        </div>
                        <div className="p-3 flex-grow-1 overflow-scroll h-100">
                            <EditAiAgentMain />
                        </div>
                        <div className="p-3 border-top border-secondary">
                            <EditAiAgentFooter />
                        </div>
                    </div>
                    <div
                        style={{
                            width: `${testAreaResizable.width}px`,
                            position: "relative",
                            borderLeft: `1px solid ${testAreaResizable.isDragging ? "#ccc" : "#4c4c63"}`,
                        }}
                        className="panel-bg-1 h-100"
                    >
                        <ColumnResize handleMouseDown={testAreaResizable.handleMouseDown} />
                        <EditAiAgentTestResults />
                    </div>
                </div>
            </form>
        </FormProvider>
    );
}

function ColumnResize({ handleMouseDown }: { handleMouseDown: (e: React.MouseEvent) => void }) {
    return (
        <div
            style={{
                position: "absolute",
                left: "-5px",
                top: 0,
                bottom: 0,
                width: "10px",
                cursor: "col-resize",
            }}
            onMouseDown={handleMouseDown}
        />
    );
}
