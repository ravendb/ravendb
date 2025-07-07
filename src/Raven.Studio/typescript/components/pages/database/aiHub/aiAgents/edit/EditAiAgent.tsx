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
import genUtils from "common/generalUtils";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import router from "plugins/router";
import { useAppUrls } from "components/hooks/useAppUrls";
import { editAiAgentActions } from "./store/editAiAgentSlice";
import { useEffect } from "react";

interface QueryParams {
    agentName: string;
}

export default function EditAiAgent({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { aiAgentService } = useServices();

    const form = useForm<EditAiAgentFormData>({
        defaultValues: async () => {
            if (!queryParams?.agentName) {
                return mapFromDto(null);
            }

            const agent = await aiAgentService.getAiAgents(databaseName, queryParams.agentName);

            return mapFromDto(queryParams.agentName, agent);
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
            await aiAgentService.saveAiAgent(databaseName, formData.name, mapToDto(formData));

            reset(formData);
            setIsDirty(false);
            router.navigate(appUrl.forAiAgents(databaseName));
        });
    };

    useEffect(() => {
        return () => {
            dispatch(editAiAgentActions.reset());
        };
    }, []);

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
                        className="panel-bg-1 h-100 vstack"
                    >
                        <ColumnResize handleMouseDown={testAreaResizable.handleMouseDown} />
                        <EditAiAgentTestPanel />
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

function mapFromDto(
    name: string,
    dto?: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration
): EditAiAgentFormData {
    if (!name) {
        return {
            name: "",
            connectionStringName: "",
            systemPrompt: "",
            outputSchema: "",
            persistenceCollectionName: "",
            persistenceExpiresInSeconds: 2592000, // 30 days
            parameters: [],
            queries: [],
            actions: [],
            testPrompt: "",
        };
    }

    return {
        name,
        connectionStringName: dto.ConnectionStringName,
        systemPrompt: dto.SystemPrompt,
        outputSchema: dto.OutputSchema,
        persistenceCollectionName: dto.Persistence.Collection,
        persistenceExpiresInSeconds: genUtils.timeSpanToSeconds(dto.Persistence.Expires),
        parameters: [], // TODO: map parameters
        queries: dto.Queries.map((x) => ({
            name: x.Name,
            description: x.Description,
            query: x.Query,
            parametersSchema: x.ParametersSchema,
            isSaved: true,
            isEditing: false,
        })),
        actions: dto.Actions.map((x) => ({
            name: x.Name,
            description: x.Description,
            parametersSchema: x.ParametersSchema,
            isSaved: true,
            isEditing: false,
        })),
        testPrompt: "",
    };
}

function mapToDto(formData: EditAiAgentFormData): Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration {
    return {
        ConnectionStringName: formData.connectionStringName,
        SystemPrompt: formData.systemPrompt,
        OutputSchema: formData.outputSchema,
        Persistence: {
            Collection: formData.persistenceCollectionName,
            Expires: genUtils.formatAsTimeSpan(formData.persistenceExpiresInSeconds * 1000),
        },
        Queries: formData.queries.map((x) => ({
            Name: x.name,
            Description: x.description,
            Query: x.query,
            ParametersSchema: x.parametersSchema,
        })),
        Actions: formData.actions.map((x) => ({
            Name: x.name,
            Description: x.description,
            ParametersSchema: x.parametersSchema,
        })),
    };
}
