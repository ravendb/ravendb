import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/wizard-store";
import { appSchema, type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { api } from "@/api/api";
import { useMutation } from "@tanstack/react-query";
import { useAppSteps, useAppFlow } from "@/pages/setup/add-app-wizard/app-wizard-flow";
import { redirect } from "react-router";
import { appRoutes } from "@/lib/app-routes";

export function AddAppWizard() {
    const resetStore = useSetupWizardStore((state) => state.reset);

    const form = useForm<AppFormData>({
        mode: "onChange",
        defaultValues: {
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
            },
        },
        resolver: zodResolver(appSchema),
    });

    useEffect(() => {
        resetStore();
        return resetStore;
    }, [resetStore]);

    const provision = useMutation({
        mutationFn: async (formValues: AppFormData) => {
            await api.services.setup.provision({
                appName: formValues.externalConnection.appName,
            });
        },
    });

    return (
        <FormProvider {...form}>
            <form
                onSubmit={form.handleSubmit(async (formValues) => await provision.mutateAsync(formValues))}
                className="h-full"
            >
                <AddAppWizardBody />
            </form>
        </FormProvider>
    );
}

function AddAppWizardBody() {
    const steps = useAppSteps();
    const flow = useAppFlow();

    return <FormWizard steps={steps} flow={flow} cancel={() => redirect(appRoutes.dashboard())} />;
}
