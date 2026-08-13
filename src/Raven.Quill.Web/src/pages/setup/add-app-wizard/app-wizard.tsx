import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect, useState } from "react";
import { FormProvider, useForm, useFormContext, useWatch } from "react-hook-form";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { FormWizard, type WizardCompletion } from "@/components/form/wizard/form-wizard";
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
import { cancelAbandonedSuggestions } from "@/pages/setup/add-app-wizard/steps/map-tables/suggest-map-tables-query";
import { VERIFY_CDC_QUERY_KEY } from "@/pages/setup/add-app-wizard/steps/verify/verify-cdc-query";

/** The app the wizard is editing, instead of creating a new one. */
export type EditedApp = {
    slug: string;
    /** Schemas the stored mapping references, so discovery reaches beyond the default one. */
    discoverSchemas: string[];
};

type AppWizardProps = {
    defaultValues: AppFormData;
    editedApp?: EditedApp;
};

type CreatedApp = { slug: string; name: string };

export function AppWizard({ defaultValues, editedApp }: AppWizardProps) {
    const resetStore = useSetupWizardStore((state) => state.reset);
    const startEditingApp = useSetupWizardStore((state) => state.startEditingApp);
    const navigate = useNavigate();
    const queryClient = useQueryClient();
    const [createdApp, setCreatedApp] = useState<CreatedApp | null>(null);
    // The wizard hands over to a dialog after creating, so the draft outlives the submit that saved it.
    const [isSaved, setIsSaved] = useState(false);
    // Seeded once: a later prop identity change must not discard the operator's work.
    const [editedAppSeed] = useState(() => editedApp);
    const isEditing = editedAppSeed !== undefined;

    const appsQuery = useQuery(api.queries.apps.list());
    const takenSlugs = (appsQuery.data ?? []).map((app) => app.slug).filter((slug) => slug !== editedAppSeed?.slug);

    const form = useForm<AppFormData>({
        mode: "onChange",
        defaultValues,
        resolver: async (values, context, options) => {
            const flow = getAppFlow({ dataSource: values.dataSource?.source, isEditing });
            return zodResolver(buildAppSchemaForFlow(flow, takenSlugs))(values, context, options);
        },
    });

    useEffect(() => {
        // A cached dry run must not outlive the session: a reopened wizard would light up
        // "Schema verified" for a run the operator never saw.
        queryClient.removeQueries({ queryKey: VERIFY_CDC_QUERY_KEY });
        resetStore();

        if (editedAppSeed) {
            startEditingApp(editedAppSeed.slug, editedAppSeed.discoverSchemas);
        }

        return () => {
            cancelAbandonedSuggestions(queryClient);
            queryClient.removeQueries({ queryKey: VERIFY_CDC_QUERY_KEY });
            resetStore();
        };
    }, [editedAppSeed, queryClient, resetStore, startEditingApp]);

    const provisionMutation = useMutation({
        mutationFn: async (formValues: AppFormData) => {
            return await api.services.setup.provision({
                appName: formValues.externalConnection.appName,
                slug: formValues.externalConnection.slug || null,
            });
        },
        onSuccess: async (result, formValues) => {
            setIsSaved(true);

            if (editedAppSeed) {
                await invalidateAppQueries(queryClient, result.slug);
                toast.success(`App ${result.slug} updated`);
                navigate(appRoutes.app(result.slug, "data-source"));
                return;
            }

            await invalidateAppQueries(queryClient);
            toast.success(`App ${result.slug} created`);
            setCreatedApp({ slug: result.slug, name: formValues.externalConnection.appName });
        },
        onError: (error) => {
            const message = error instanceof Error ? error.message.split("\n")[0] : "Could not save app.";
            // 400 = invalid/reserved slug, 409 = slug already taken. Pinned to the slug field so it
            // shows when the operator navigates back to the connect step.
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
                <AppWizardBody
                    cancel={() => navigate(editedAppSeed ? appRoutes.app(editedAppSeed.slug) : appRoutes.dashboard())}
                    completion={isEditing ? EDIT_COMPLETION : CREATE_COMPLETION}
                    isEditing={isEditing}
                    isSaved={isSaved}
                />
            </form>
            <AppCreatedDialog
                app={createdApp}
                onContinue={() => createdApp && navigate(appRoutes.addCapability(createdApp.slug, "agent"))}
            />
        </FormProvider>
    );
}

const CREATE_COMPLETION: WizardCompletion = {
    type: "submit",
    label: "Create app & continue",
    busyLabel: "Creating app...",
};

const EDIT_COMPLETION: WizardCompletion = {
    type: "submit",
    label: "Save changes",
    busyLabel: "Saving changes...",
};

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

function AppWizardBody({
    cancel,
    completion,
    isEditing,
    isSaved,
}: {
    cancel: () => void;
    completion: WizardCompletion;
    isEditing: boolean;
    isSaved: boolean;
}) {
    const steps = useAppSteps();
    const { control } = useFormContext<AppFormData>();

    const dataSource = useWatch({
        control,
        name: "dataSource.source",
    });

    const flow = getAppFlow({ dataSource, isEditing });

    return <FormWizard steps={steps} flow={flow} cancel={cancel} completion={completion} isSaved={isSaved} />;
}
