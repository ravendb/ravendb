import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import useResizableWidth from "components/hooks/useResizableWidth";
import { useServices } from "components/hooks/useServices";
import { connectionStringsActions } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { defaultItemsToProcess } from "components/pages/database/settings/documentExpiration/DocumentExpiration";
import { useAppDispatch, useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { useEffect } from "react";
import { useAsyncCallback } from "react-async-hook";
import { useForm, useWatch, useFieldArray, SubmitHandler } from "react-hook-form";
import { editAiAgentSelectors, editAiAgentActions } from "../store/editAiAgentSlice";
import { editAiAgentUtils } from "../utils/editAiAgentUtils";
import {
    EditAiAgentFormData,
    editAiAgentYupResolver,
    ParameterAiAgentFormData,
    parameterAiAgentYupResolver,
    TestAiAgentFormData,
    testAiAgentYupResolver,
} from "../utils/editAiAgentValidation";
import router from "plugins/router";
import { TimeInSeconds } from "common/constants/timeInSeconds";

interface QueryParams {
    id: string;
    isClone?: boolean;
}

export default function useEditAiAgent(queryParams: QueryParams) {
    const dispatch = useAppDispatch();
    const { aiAgentService, databasesService } = useServices();
    const { appUrl } = useAppUrls();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isDocumentExpirationEnabled = useAppSelector(editAiAgentSelectors.isDocumentExpirationEnabled);
    const isCommunityLicense = useAppSelector(licenseSelectors.licenseType) === "Community";

    const isEditAiAgent = !!queryParams?.id && !queryParams.isClone;

    // Set connection strings view context on mount and reset store on unmount
    useEffect(() => {
        dispatch(connectionStringsActions.viewContextSet("aiConnectionStrings"));

        return () => {
            dispatch(editAiAgentActions.reset());
        };
    }, []);

    const asyncGetEditDefaultValues = useAsyncCallback(async () => {
        const isDocumentExpirationEnabled = await dispatch(
            editAiAgentActions.getIsDocumentExpirationEnabled(databaseName)
        ).unwrap();

        if (queryParams?.id) {
            const agents = await aiAgentService.getAiAgents(databaseName, queryParams.id);
            return editAiAgentUtils.mapFromDto(agents.AiAgents[0], queryParams.isClone, isDocumentExpirationEnabled);
        } else {
            return editAiAgentUtils.mapFromDto(null, false, isDocumentExpirationEnabled);
        }
    });

    const editForm = useForm<EditAiAgentFormData>({
        defaultValues: asyncGetEditDefaultValues.execute,
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

    const reloadEditForm = async () => {
        const result = await asyncGetEditDefaultValues.execute();
        editForm.reset(result);
    };

    const testAreaResizable = useResizableWidth({
        initialWidth: 500,
        minWidth: 500,
        maxWidth: 1000,
    });

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

    return {
        editForm,
        parameterForm,
        testForm,
        reloadEditForm,
        asyncGetEditDefaultValues,
        allQueriesNames,
        saveFieldsAndSubmit,
        testAreaResizable,
        isEditAiAgent,
        handleSubmitParameter: parameterForm.handleSubmit(handleAddParameter),
        parametersFieldArray,
    };
}

const minimumCommunityDeleteFrequencyInSec = TimeInSeconds.Day * 36;
