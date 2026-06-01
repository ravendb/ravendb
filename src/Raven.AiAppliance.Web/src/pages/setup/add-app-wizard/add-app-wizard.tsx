import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/wizard-store";
import { appSchema, useAppFlow, useAppSteps, type AppFormData } from "@/pages/setup/add-app-wizard/wizard-model";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { api } from "@/api/api";
import { useMutation } from "@tanstack/react-query";

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
            howToMap: {
                aiPrompt: "",
                source: "ai-suggested",
            },
            map: {
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

    return (
        <FormWizard
            steps={steps}
            flow={flow}
            cancel={() => {
                console.log("TODO cancel");
            }}
        />
    );
}
