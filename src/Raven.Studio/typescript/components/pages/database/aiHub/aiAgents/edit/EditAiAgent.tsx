import "./EditAiAgent.scss";
import { AboutViewHeading } from "components/common/AboutView";
import useResizableWidth from "../hooks/useResizableWidth";
import { FormProvider, SubmitHandler, useForm } from "react-hook-form";
import { EditAiAgentFormData, editAiAgentYupResolver } from "./utils/editAiAgentValidation";
import EditAiAgentMain from "./EditAiAgentMain";
import EditAiAgentFooter from "./EditAiAgentFooter";
import EditAiAgentTestPanel from "./EditAiAgentTestPanel";
import { tryHandleSubmit } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import router from "plugins/router";
import { useAppUrls } from "components/hooks/useAppUrls";
import { editAiAgentActions } from "./store/editAiAgentSlice";
import { useEffect } from "react";
import EditAiAgentInfoHub from "./EditAiAgentInfoHub";
import { editAiAgentUtils } from "./utils/editAiAgentUtils";
import SizeGetter from "components/common/SizeGetter";

interface QueryParams {
    id: string;
    isClone?: boolean;
}

export default function EditAiAgent({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { aiAgentService } = useServices();

    const form = useForm<EditAiAgentFormData>({
        defaultValues: async () => {
            console.log("kalczur queryParams", queryParams);
            if (!queryParams?.id) {
                return editAiAgentUtils.mapFromDto(null);
            }

            const agents = await aiAgentService.getAiAgents(databaseName, queryParams.id);
            return editAiAgentUtils.mapFromDto(agents[0], queryParams.isClone);
        },
        resolver: editAiAgentYupResolver,
    });

    const { handleSubmit, formState, reset } = form;

    const { setIsDirty } = useDirtyFlag(formState.isDirty);

    // It's not working as expected, let's fix it later
    const testAreaResizable = useResizableWidth({
        initialWidth: 500,
        minWidth: 500,
        maxWidth: 1000,
    });

    const { appUrl } = useAppUrls();

    const saveAgent: SubmitHandler<EditAiAgentFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await aiAgentService.saveAiAgent(databaseName, editAiAgentUtils.mapToDto(formData));

            reset(formData);
            setIsDirty(false);
            router.navigate(appUrl.forAiAgents(databaseName));
        });
    };

    // Reset slice on unmount
    useEffect(() => {
        return () => {
            dispatch(editAiAgentActions.reset());
        };
    }, []);

    return (
        <FormProvider {...form}>
            <form onSubmit={handleSubmit(saveAgent)} className="h-100">
                <SizeGetter
                    render={({ width }) => (
                        <div className="hstack h-100">
                            <div className="vstack h-100" style={{ width: `${width - testAreaResizable.width}px` }}>
                                <div className="hstack justify-content-between align-items-start p-3">
                                    <AboutViewHeading title="Create AI Agent" icon="ai-agents" marginBottom={0} />
                                    <EditAiAgentInfoHub />
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
                                className="panel-bg-1 h-100 vstack"
                            >
                                <ColumnResize handleMouseDown={testAreaResizable.handleMouseDown} />
                                <EditAiAgentTestPanel />
                            </div>
                        </div>
                    )}
                />
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
