import "./EditAiAgent.scss";
import { AboutViewHeading } from "components/common/AboutView";
import useResizableWidth from "../hooks/useResizableWidth";
import { FormProvider, SubmitHandler, useForm } from "react-hook-form";
import { EditAiAgentFormData, editAiAgentYupResolver } from "./utils/editAiAgentValidation";
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
import { TimeInSeconds } from "common/constants/timeInSeconds";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { defaultItemsToProcess } from "components/pages/database/settings/documentExpiration/DocumentExpiration";
import EditAiAgentBasicSection from "./partials/EditAiAgentBasicSection";
import EditAiAgentParametersSection from "./partials/EditAiAgentParametersSection";
import EditAiAgentPersistenceSection from "./partials/EditAiAgentPersistenceSection";
import EditAiAgentToolsSection from "./partials/EditAiAgentToolsSection";
import EditAiAgentTrimmingSection from "./partials/EditAiAgentTrimmingSection";
import EditAiAgentToolsAdvancedSection from "./partials/EditAiAgentToolsAdvancedSection";
import { LoadingView } from "components/common/LoadingView";

interface QueryParams {
    id: string;
    isClone?: boolean;
}

export default function EditAiAgent({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { aiAgentService, databasesService } = useServices();

    const form = useForm<EditAiAgentFormData>({
        defaultValues: async () => {
            const isDocumentExpirationEnabled = await dispatch(
                editAiAgentActions.getIsDocumentExpirationEnabled(databaseName)
            ).unwrap();

            if (queryParams?.id) {
                const agents = await aiAgentService.getAiAgents(databaseName, queryParams.id);
                return editAiAgentUtils.mapFromDto(agents[0], queryParams.isClone, isDocumentExpirationEnabled);
            } else {
                return editAiAgentUtils.mapFromDto(null, false, isDocumentExpirationEnabled);
            }
        },
        resolver: editAiAgentYupResolver,
    });

    const { handleSubmit, formState, reset } = form;

    const { setIsDirty } = useDirtyFlag(formState.isDirty);

    const testAreaResizable = useResizableWidth({
        initialWidth: 500,
        minWidth: 500,
        maxWidth: 1000,
    });

    const { appUrl } = useAppUrls();
    const isDocumentExpirationEnabled = useAppSelector(editAiAgentSelectors.isDocumentExpirationEnabled);
    const isCommunityLicense = useAppSelector(licenseSelectors.licenseType) === "Community";

    const saveAgent: SubmitHandler<EditAiAgentFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            if (
                isDocumentExpirationEnabled.status === "success" &&
                !isDocumentExpirationEnabled.data &&
                formData.isEnableDocumentExpiration
            ) {
                await databasesService.saveExpirationConfiguration(databaseName, {
                    Disabled: false,
                    DeleteFrequencyInSec: isCommunityLicense ? minimumCommunityDeleteFrequencyInSec : null,
                    MaxItemsToProcess: defaultItemsToProcess,
                });
            }

            await aiAgentService.saveAiAgent(
                databaseName,
                editAiAgentUtils.mapToDto(formData, isDocumentExpirationEnabled.data)
            );

            reset(formData);
            setIsDirty(false);
            router.navigate(appUrl.forAiAgents(databaseName));
        });
    };

    // Reset on unmount
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
                                <div className="hstack justify-content-between align-items-start p-4">
                                    <AboutViewHeading title="Create AI Agent" icon="ai-agents" marginBottom={0} />
                                    <EditAiAgentInfoHub />
                                </div>
                                <div className="px-4 pb-4 flex-grow-1 overflow-scroll h-100">
                                    {formState.isLoading ? (
                                        <LoadingView />
                                    ) : (
                                        <>
                                            <EditAiAgentBasicSection />
                                            <EditAiAgentPersistenceSection />
                                            <EditAiAgentParametersSection />
                                            <EditAiAgentToolsSection />
                                            <EditAiAgentToolsAdvancedSection />
                                            <EditAiAgentTrimmingSection />
                                        </>
                                    )}
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

const minimumCommunityDeleteFrequencyInSec = TimeInSeconds.Day * 36;
