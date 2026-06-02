import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect } from "react";
import { FormProvider, useForm, useFormContext, useWatch } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { useAppSteps, buildAppSchemaForFlow, getAppFlow } from "@/pages/setup/add-app-wizard/app-wizard-flow";
import { useNavigate } from "react-router";
import { appRoutes } from "@/lib/app-routes";
import { api } from "@/api/api";
import { useMutation } from "@tanstack/react-query";

export function AddAppWizard() {
    const resetStore = useSetupWizardStore((state) => state.reset);
    const navigate = useNavigate();

    const form = useForm<AppFormData>({
        mode: "onChange",
        defaultValues: getDefaultValues(),
        resolver: async (values, context, options) => {
            const flow = getAppFlow({
                dataSource: values.dataSource?.source,
                mapSource: values.map?.source,
            });
            return zodResolver(buildAppSchemaForFlow(flow))(values, context, options);
        },
    });

    useEffect(() => {
        resetStore();
        return resetStore;
    }, [resetStore]);

    const provisionMutation = useMutation({
        mutationFn: async (formValues: AppFormData) => {
            const result = await api.services.setup.provision({
                appName: formValues.externalConnection.appName,
            });

            navigate(appRoutes.app(result.slug));
        },
    });

    return (
        <FormProvider {...form}>
            <form onSubmit={form.handleSubmit((x) => provisionMutation.mutateAsync(x))} className="h-full">
                <AddAppWizardBody />
            </form>
        </FormProvider>
    );
}

function AddAppWizardBody() {
    const steps = useAppSteps();
    const navigate = useNavigate();
    const { control } = useFormContext<AppFormData>();

    const dataSource = useWatch({
        control,
        name: "dataSource.source",
    });

    const mapSource = useWatch({
        control,
        name: "map.source",
    });

    const flow = getAppFlow({
        dataSource,
        mapSource,
    });

    return (
        <FormWizard
            steps={steps}
            flow={flow}
            cancel={() => {
                navigate(appRoutes.dashboard());
            }}
        />
    );
}

function getDefaultValues(): AppFormData {
    return {
        dataSource: {
            source: "external",
        },
        externalConnection: {
            appName: "",
            provider: "Npgsql",
            connectionString: "",
        },
        verifySchema: {
            tables: [],
        },
        map: {
            source: "ai-suggested",
            aiPrompt: "",
        },
        mapAiSuggest: {
            tables: [],
        },
        mapManual: {
            tables: [],
        },
        preview: {
            table: "",
            maxRows: 1,
        },
    };
}
