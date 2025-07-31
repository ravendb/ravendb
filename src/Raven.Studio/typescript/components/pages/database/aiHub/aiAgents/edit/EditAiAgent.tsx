import "./EditAiAgent.scss";
import { AboutViewHeading } from "components/common/AboutView";
import useResizableWidth from "components/hooks/useResizableWidth";
import { FormProvider, SubmitHandler, useFieldArray, useForm, useWatch } from "react-hook-form";
import {
    EditAiAgentFormData,
    editAiAgentYupResolver,
    ParameterAiAgentFormData,
    parameterAiAgentYupResolver,
    TestAiAgentFormData,
    testAiAgentYupResolver,
} from "./utils/editAiAgentValidation";
import EditAiAgentFooter from "./partials/EditAiAgentFooter";
import EditAiAgentTestPanel from "./partials/EditAiAgentTestPanel";
import { tryHandleSubmit } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import router from "plugins/router";
import { useAppUrls } from "components/hooks/useAppUrls";
import { editAiAgentActions, editAiAgentSelectors } from "./store/editAiAgentSlice";
import { useEffect } from "react";
import EditAiAgentInfoHub from "./partials/EditAiAgentInfoHub";
import { editAiAgentUtils } from "./utils/editAiAgentUtils";
import SizeGetter from "components/common/SizeGetter";
import EditAiAgentBasicSection from "./partials/EditAiAgentBasicSection";
import EditAiAgentParametersSection from "./partials/EditAiAgentParametersSection";
import EditAiAgentToolsSection from "./partials/EditAiAgentToolsSection";
import EditAiAgentTrimmingSection from "./partials/EditAiAgentTrimmingSection";
import { LoadingView } from "components/common/LoadingView";
import { connectionStringsActions } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { useAsyncCallback } from "react-async-hook";
import { LoadError } from "components/common/LoadError";

interface QueryParams {
    id: string;
    isClone?: boolean;
}

export default function EditAiAgent({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();
    const { aiAgentService } = useServices();
    const { appUrl } = useAppUrls();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isTestOpen = useAppSelector(editAiAgentSelectors.isTestOpen);

    const isEditAiAgent = !!queryParams?.id && !queryParams.isClone;

    const asyncGetDefaultValues = useAsyncCallback(async () => {
        if (queryParams?.id) {
            const agents = await aiAgentService.getAiAgents(databaseName, queryParams.id);
            return editAiAgentUtils.mapFromDto(agents.AiAgents[0], queryParams.isClone);
        } else {
            return editAiAgentUtils.mapFromDto(null, false);
        }
    });

    const editForm = useForm<EditAiAgentFormData>({
        defaultValues: asyncGetDefaultValues.execute,
        resolver: editAiAgentYupResolver,
    });

    const editFormValues = useWatch({
        control: editForm.control,
    });

    const parameterForm = useForm<ParameterAiAgentFormData>({
        defaultValues: {
            nameInput: "",
            descriptionInput: null,
        },
        resolver: parameterAiAgentYupResolver,
        context: {
            allParameterNames: editFormValues.parameters?.map((x) => x.name) ?? [],
        },
    });

    const parameterFormValues = useWatch({
        control: parameterForm.control,
    });

    const parametersFieldArray = useFieldArray({
        name: "parameters",
        control: editForm.control,
    });

    const handleAddParameter: SubmitHandler<ParameterAiAgentFormData> = async (formData) => {
        parametersFieldArray.append({
            name: formData.nameInput,
            description: formData.descriptionInput,
        });
        parameterForm.reset();
    };

    const allQueriesNames = editFormValues.queries?.map((x) => x.name) ?? [];

    const testForm = useForm<TestAiAgentFormData>({
        defaultValues: {
            prompt: "",
            parameters: [],
        },
        resolver: testAiAgentYupResolver,
    });

    const { setIsDirty } = useDirtyFlag(editForm.formState.isDirty);

    const reloadForm = async () => {
        const result = await asyncGetDefaultValues.execute();
        editForm.reset(result);
    };

    // Set connection strings view context
    useEffect(() => {
        dispatch(connectionStringsActions.viewContextSet("aiConnectionStrings"));
    }, []);

    const testAreaResizable = useResizableWidth({
        initialWidth: 500,
        minWidth: 500,
        maxWidth: 1000,
    });

    const saveAgent: SubmitHandler<EditAiAgentFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await aiAgentService.saveAiAgent(databaseName, editAiAgentUtils.mapToDto(formData));

            editForm.reset(formData);
            setIsDirty(false);
            router.navigate(appUrl.forAiAgents(databaseName));
        });
    };

    const saveFieldsAndSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault();

        if (parameterFormValues.nameInput) {
            await parameterForm.handleSubmit(handleAddParameter)();
        }

        await editForm.handleSubmit(saveAgent)();
    };

    // Reset store on unmount
    useEffect(() => {
        return () => {
            dispatch(editAiAgentActions.reset());
        };
    }, []);

    return (
        <FormProvider {...editForm}>
            <form onSubmit={saveFieldsAndSubmit} className="h-100 edit-ai-agent">
                <SizeGetter
                    render={({ width }) => (
                        <div className="hstack h-100">
                            <div
                                className="vstack h-100"
                                style={{ width: isTestOpen ? `${width - testAreaResizable.width}px` : "100%" }}
                            >
                                <div className="hstack justify-content-between align-items-start p-4">
                                    <AboutViewHeading
                                        title={`${isEditAiAgent ? "Edit" : "Create"} AI Agent`}
                                        icon="ai-agents"
                                        marginBottom={0}
                                    />
                                    <EditAiAgentInfoHub />
                                </div>
                                <div className="px-4 pb-4 flex-grow-1 overflow-scroll h-100">
                                    {asyncGetDefaultValues.loading && <LoadingView />}
                                    {asyncGetDefaultValues.error && (
                                        <LoadError error="Unable to load configuration" refresh={reloadForm} />
                                    )}
                                    {asyncGetDefaultValues.result && (
                                        <>
                                            <EditAiAgentBasicSection isEditAiAgent={isEditAiAgent} />
                                            <EditAiAgentParametersSection
                                                control={parameterForm.control}
                                                parametersFieldArray={parametersFieldArray}
                                                handleSubmit={parameterForm.handleSubmit(handleAddParameter)}
                                            />
                                            <EditAiAgentToolsSection />
                                            <EditAiAgentTrimmingSection />
                                        </>
                                    )}
                                </div>
                                <div className="p-3 border-top border-secondary">
                                    <EditAiAgentFooter testForm={testForm} editForm={editForm} />
                                </div>
                            </div>
                            {isTestOpen && (
                                <div
                                    style={{
                                        width: `${testAreaResizable.width}px`,
                                        position: "relative",
                                        borderLeft: `1px solid ${testAreaResizable.isDragging ? "#ccc" : "#4c4c63"}`,
                                    }}
                                    className="panel-bg-1 h-100 vstack"
                                >
                                    <ColumnResize handleMouseDown={testAreaResizable.handleMouseDown} />
                                    <EditAiAgentTestPanel
                                        testForm={testForm}
                                        editForm={editForm}
                                        allQueriesNames={allQueriesNames}
                                    />
                                </div>
                            )}
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
