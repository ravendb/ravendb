import "./EditAiAgent.scss";
import { AboutViewHeading } from "components/common/AboutView";
import { FormProvider } from "react-hook-form";
import EditAiAgentFooter from "./partials/EditAiAgentFooter";
import EditAiAgentTestPanel from "./partials/EditAiAgentTestPanel";
import { useAppSelector } from "components/store";
import { editAiAgentSelectors } from "./store/editAiAgentSlice";
import EditAiAgentInfoHub from "./partials/EditAiAgentInfoHub";
import SizeGetter from "components/common/SizeGetter";
import EditAiAgentBasicSection from "./partials/EditAiAgentBasicSection";
import EditAiAgentParametersSection from "./partials/EditAiAgentParametersSection";
import EditAiAgentToolsSection from "./partials/EditAiAgentToolsSection";
import EditAiAgentTrimmingSection from "./partials/EditAiAgentTrimmingSection";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import useEditAiAgent from "./hooks/useEditAiAgent";
import useResizableWidth from "components/hooks/useResizableWidth";
import ColumnResize from "components/common/ColumnResize";
import EditAiAgentSubAgentsSection from "components/pages/database/aiHub/aiAgents/edit/partials/EditAiAgentSubAgentsSection";

interface QueryParams {
    id: string;
    isClone?: boolean;
}

export default function EditAiAgent({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const aiAgentEditor = useEditAiAgent(queryParams);

    return (
        <FormProvider {...aiAgentEditor.editForm}>
            <form onSubmit={aiAgentEditor.handleSubmit} className="h-100 edit-ai-agent">
                <SizeGetter render={({ width }) => <FormBody maxWidth={width} aiAgentEditor={aiAgentEditor} />} />
            </form>
        </FormProvider>
    );
}

interface FormBodyProps {
    maxWidth: number;
    aiAgentEditor: ReturnType<typeof useEditAiAgent>;
}

function FormBody({ maxWidth, aiAgentEditor }: FormBodyProps) {
    const isTestOpen = useAppSelector(editAiAgentSelectors.isTestOpen);
    const isTestPinned = useAppSelector(editAiAgentSelectors.isTestPinned);

    const testAreaResizable = useResizableWidth({
        initialWidth: maxWidth / 2,
        minWidth: 300,
        maxWidth: isTestPinned ? maxWidth - 400 : maxWidth - 50,
    });

    return (
        <div className="hstack h-100 ai-agents-config" style={{ position: "relative" }}>
            <div
                className="vstack h-100"
                style={{
                    width: isTestOpen && isTestPinned ? `${maxWidth - testAreaResizable.width}px` : "100%",
                }}
            >
                <div className="hstack justify-content-between align-items-start p-4">
                    <AboutViewHeading
                        title={`${aiAgentEditor.isEditAiAgent ? "Edit" : "Create"} AI Agent`}
                        icon="ai-agents"
                        marginBottom={0}
                    />
                    <EditAiAgentInfoHub />
                </div>
                <div className="px-4 pb-4 flex-grow-1 overflow-scroll h-100">
                    {aiAgentEditor.asyncGetEditDefaultValues.loading && <LoadingView />}
                    {aiAgentEditor.asyncGetEditDefaultValues.error && (
                        <LoadError error="Unable to load configuration" refresh={aiAgentEditor.reloadEditForm} />
                    )}
                    {aiAgentEditor.asyncGetEditDefaultValues.result && (
                        <>
                            <EditAiAgentBasicSection isEditAiAgent={aiAgentEditor.isEditAiAgent} />
                            <EditAiAgentParametersSection />
                            <EditAiAgentToolsSection />
                            <EditAiAgentSubAgentsSection />
                            <EditAiAgentTrimmingSection />
                        </>
                    )}
                </div>
                <div className="p-3 border-top border-secondary">
                    <EditAiAgentFooter
                        editForm={aiAgentEditor.editForm}
                        generateTestParameters={aiAgentEditor.generateTestParameters}
                    />
                </div>
            </div>
            {isTestOpen && (
                <div
                    style={{
                        width: `${testAreaResizable.width}px`,
                        borderLeft: `1px solid ${testAreaResizable.isDragging ? "#ccc" : "#4c4c63"}`,
                        zIndex: 12,
                        position: "relative",
                        ...(!isTestPinned && testPanelUnpinnedStyles),
                    }}
                    className="panel-bg-1 h-100 vstack"
                >
                    <ColumnResize handleMouseDown={testAreaResizable.handleMouseDown} />
                    <EditAiAgentTestPanel
                        testForm={aiAgentEditor.testForm}
                        editForm={aiAgentEditor.editForm}
                        generateTestParameters={aiAgentEditor.generateTestParameters}
                    />
                </div>
            )}
        </div>
    );
}

const testPanelUnpinnedStyles: React.CSSProperties = {
    position: "absolute",
    right: 0,
    top: 0,
    bottom: 0,
};
