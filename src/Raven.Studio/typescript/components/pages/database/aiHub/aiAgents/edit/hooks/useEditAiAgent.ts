import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useServices } from "components/hooks/useServices";
import { connectionStringsActions } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { useEffect } from "react";
import { useAsyncCallback } from "react-async-hook";
import { useForm, SubmitHandler } from "react-hook-form";
import { editAiAgentActions } from "../store/editAiAgentSlice";
import { editAiAgentUtils } from "../utils/editAiAgentUtils";
import {
    EditAiAgentFormData,
    EditAiAgentValidationContext,
    editAiAgentYupResolver,
    TestAiAgentFormData,
    testAiAgentYupResolver,
} from "../utils/editAiAgentValidation";
import router from "plugins/router";
import { hasRelevantDirtyFields } from "components/common/Form";

interface QueryParams {
    id: string;
    isClone?: boolean;
}

export default function useEditAiAgent(queryParams: QueryParams) {
    const dispatch = useAppDispatch();
    const { aiAgentService } = useServices();
    const { appUrl } = useAppUrls();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const isEditAiAgent = !!queryParams?.id && !queryParams.isClone;

    // Set connection strings view context on mount and reset store on unmount
    useEffect(() => {
        dispatch(connectionStringsActions.viewContextSet("aiTask"));
        dispatch(editAiAgentActions.getAllIdentifiers(databaseName));

        return () => {
            dispatch(editAiAgentActions.reset());
        };
    }, []);

    const asyncGetEditDefaultValues = useAsyncCallback(async () => {
        if (queryParams?.id) {
            const agents = await aiAgentService.getAiAgents(databaseName, queryParams.id);
            return editAiAgentUtils.mapFromDto(agents.AiAgents[0], queryParams.isClone);
        } else {
            return editAiAgentUtils.mapFromDto(null, false);
        }
    });

    const editForm = useForm<EditAiAgentFormData>({
        defaultValues: asyncGetEditDefaultValues.execute,
        resolver: (data, _, options) =>
            editAiAgentYupResolver(
                data,
                {
                    allParameterNames: data.parameters?.map((x) => x.name) ?? [],
                    allQueryNames: data.queries?.map((x) => x.name) ?? [],
                    allActionNames: data.actions?.map((x) => x.name) ?? [],
                } satisfies EditAiAgentValidationContext,
                options
            ),
    });

    const testForm = useForm<TestAiAgentFormData>({
        defaultValues: {
            prompt: "",
            parameters: [],
        },
        resolver: testAiAgentYupResolver,
    });

    const generateTestParameters = () => {
        testForm.setValue(
            "parameters",
            editForm.getValues().parameters.map((configParam): TestAiAgentFormData["parameters"][number] => {
                const persistedParameter = testForm
                    .getValues()
                    .parameters.find((testParam) => testParam.name === configParam.name);

                return {
                    name: configParam.name,
                    type: configParam.type,
                    isSendToModel: persistedParameter?.isSendToModel ?? configParam.isSendToModel,
                    value: persistedParameter?.value ?? null,
                };
            })
        );
    };

    const hasRelevantDirty = hasRelevantDirtyFields(editForm.formState.dirtyFields, ["isEditing"]);
    const { setIsDirty } = useDirtyFlag(hasRelevantDirty);

    const reloadEditForm = async () => {
        const result = await asyncGetEditDefaultValues.execute();
        editForm.reset(result);
    };

    const saveAgent: SubmitHandler<EditAiAgentFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await aiAgentService.saveAiAgent(databaseName, editAiAgentUtils.mapToDto(formData));

            editForm.reset(formData);
            setIsDirty(false);
            router.navigate(appUrl.forAiAgents(databaseName));
        });
    };

    return {
        editForm,
        testForm,
        reloadEditForm,
        asyncGetEditDefaultValues,
        handleSubmit: editForm.handleSubmit(saveAgent),
        isEditAiAgent,
        generateTestParameters,
    };
}
