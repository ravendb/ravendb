import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect } from "react";
import { FormProvider, useForm, useFormContext, useWatch } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { buildAppSchemaForFlow, getAppFlow, useAppSteps } from "@/pages/setup/add-app-wizard/app-wizard-flow";
import { useNavigate } from "react-router";
import { appRoutes } from "@/lib/app-routes";
import { api } from "@/api/api";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { preventEnterKeySubmission } from "@/lib/form-utils";
import { toast } from "sonner";

export function AddAppWizard() {
    const resetStore = useSetupWizardStore((state) => state.reset);
    const navigate = useNavigate();
    const queryClient = useQueryClient();

    const form = useForm<AppFormData>({
        mode: "onChange",
        defaultValues: getDefaultValues(),
        resolver: async (values, context, options) => {
            const flow = getAppFlow({
                dataSource: values.dataSource?.source,
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
            return await api.services.setup.provision({
                appName: formValues.externalConnection.appName,
            });
        },
        onSuccess: async (result) => {
            await queryClient.invalidateQueries({ queryKey: api.queries.apps.list().queryKey });
            toast.success(`App ${result.slug} created`);
            navigate(appRoutes.app(result.slug));
        },
        onError: (error) => {
            const message = error instanceof Error ? error.message.split("\n")[0] : "Could not create app.";
            toast.error(message);
        },
    });

    return (
        <FormProvider {...form}>
            <form
                onSubmit={form.handleSubmit((x) => provisionMutation.mutateAsync(x))}
                onKeyDown={preventEnterKeySubmission}
                className="h-full"
            >
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

    const flow = getAppFlow({
        dataSource,
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
        mapTables: {
            tables: [],
        },
        preview: {
            table: "",
            maxRows: 1,
        },
    };
}
