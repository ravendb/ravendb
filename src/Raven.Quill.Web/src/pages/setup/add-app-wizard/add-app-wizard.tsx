import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect, useState } from "react";
import { FormProvider, useForm, useFormContext, useWatch } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { FormWizard } from "@/components/form/wizard/form-wizard";
import { buildAppSchemaForFlow, getAppFlow, useAppSteps } from "@/pages/setup/add-app-wizard/app-wizard-flow";
import { useNavigate } from "react-router";
import { appRoutes } from "@/lib/app-routes";
import { api } from "@/api/api";
import { isApiError } from "@/api/http-client";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { preventEnterKeySubmission } from "@/lib/form-utils";
import { invalidateAppQueries } from "@/lib/query-invalidation";
import { toast } from "sonner";
import { Button } from "@/components/shadcn/ui/button";
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
} from "@/components/shadcn/ui/dialog";
import { CdcPerformanceSection } from "@/pages/apps/cdc-performance-section";

type CreatedApp = { slug: string; name: string };

export function AddAppWizard() {
    const resetStore = useSetupWizardStore((state) => state.reset);
    const navigate = useNavigate();
    const queryClient = useQueryClient();
    const [createdApp, setCreatedApp] = useState<CreatedApp | null>(null);

    const appsQuery = useQuery(api.queries.apps.list());
    const takenSlugs = appsQuery.data?.map((app) => app.slug) ?? [];

    const form = useForm<AppFormData>({
        mode: "onChange",
        defaultValues: getDefaultValues(),
        resolver: async (values, context, options) => {
            const flow = getAppFlow({ dataSource: values.dataSource?.source });
            return zodResolver(buildAppSchemaForFlow(flow, takenSlugs))(values, context, options);
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
                slug: formValues.externalConnection.slug || null,
            });
        },
        onSuccess: async (result, formValues) => {
            await invalidateAppQueries(queryClient);
            toast.success(`App ${result.slug} created`);
            setCreatedApp({ slug: result.slug, name: formValues.externalConnection.appName });
        },
        onError: (error) => {
            const message = error instanceof Error ? error.message.split("\n")[0] : "Could not create app.";
            // 400 = invalid/reserved slug, 409 = slug already taken; pin the message to the slug
            // field so it shows when the operator navigates back to the connect step.
            if (isApiError(error) && (error.status === 400 || error.status === 409)) {
                form.setError("externalConnection.slug", { type: "server", message });
            }
            toast.error(message);
        },
    });

    return (
        <FormProvider {...form}>
            <form
                onSubmit={form.handleSubmit(async (values) => await provisionMutation.mutateAsync(values))}
                onKeyDown={preventEnterKeySubmission}
                className="h-full"
            >
                <AddAppWizardBody />
            </form>
            <AppCreatedDialog
                app={createdApp}
                onContinue={() => createdApp && navigate(appRoutes.addCapability(createdApp.slug, "agent"))}
            />
        </FormProvider>
    );
}

function AppCreatedDialog({ app, onContinue }: { app: CreatedApp | null; onContinue: () => void }) {
    return (
        <Dialog open={app !== null} onOpenChange={(open) => !open && onContinue()}>
            {app && (
                <DialogContent className="sm:max-w-xl" showCloseButton={false}>
                    <DialogHeader>
                        <DialogTitle>App &ldquo;{app.name}&rdquo; created</DialogTitle>
                        <DialogDescription>
                            We&rsquo;re syncing your data in the background. This can take a while, and you don&rsquo;t
                            have to wait for it to finish.
                        </DialogDescription>
                    </DialogHeader>
                    <CdcPerformanceSection
                        slug={app.slug}
                        title="Sync progress"
                        loadingLabel="Connecting to the live data sync..."
                        errorTitle="Could not connect to the live data sync"
                    />
                    <DialogFooter>
                        <Button onClick={onContinue}>Continue</Button>
                    </DialogFooter>
                </DialogContent>
            )}
        </Dialog>
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

    const flow = getAppFlow({ dataSource });

    return (
        <FormWizard
            steps={steps}
            flow={flow}
            cancel={() => {
                navigate(appRoutes.dashboard());
            }}
            completion={{ type: "submit", label: "Create app & continue", busyLabel: "Creating app..." }}
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
            slug: "",
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
