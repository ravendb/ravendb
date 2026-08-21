import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import type { UseFormReturn } from "react-hook-form";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { LogConfigurationResponse } from "@/api/generated/server-api";
import { isApiError } from "@/api/http-client";
import type { UnsavedChangesHandle } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { toFormValues, toUpdateRequest, type LogConfigurationFormValues } from "./log-configuration-form-values";

// A settings change is only trustworthy once the appliance has echoed it back: ResolvePath turns a
// relative directory absolute, so the response carries the path that is actually in use.
async function refetchConfiguration(queryClient: ReturnType<typeof useQueryClient>) {
    try {
        return await queryClient.fetchQuery({ ...api.queries.settings.logConfiguration(), staleTime: 0 });
    } catch {
        return undefined;
    }
}

/**
 * The save path, shared by both page layouts so they cannot drift apart while the two are compared.
 * Owns the partial-success handling, the reseed, and the confirmation gate for switching the log
 * file off.
 */
export function useLogConfigurationSave(
    configuration: LogConfigurationResponse,
    form: UseFormReturn<LogConfigurationFormValues>,
    unsavedChanges: UnsavedChangesHandle,
    /** Runs once the appliance has taken the change, including the partial save. A rejected save does
     *  not call it: the value stays in the field to be corrected. */
    onSaved?: () => void,
) {
    const queryClient = useQueryClient();
    const [partialSaveMessage, setPartialSaveMessage] = useState<string | null>(null);
    const [valuesAwaitingConfirmation, setValuesAwaitingConfirmation] = useState<LogConfigurationFormValues | null>(
        null,
    );

    const liveDirectory = configuration.logs.path ?? "";

    async function reseed() {
        const fresh = await refetchConfiguration(queryClient);
        if (fresh) {
            form.reset(toFormValues(fresh));
        }
        unsavedChanges.markSaved();
    }

    const saveMutation = useMutation({
        mutationFn: (values: LogConfigurationFormValues) =>
            api.services.settings.updateLogConfiguration(toUpdateRequest(values, configuration)),
        onSuccess: async (_result, values) => {
            setPartialSaveMessage(null);
            await reseed();
            onSaved?.();
            toast.success(
                configuration.canPersist && values.shouldPersist ? "Log settings saved" : "Log settings applied",
            );
        },
        onError: async (error) => {
            // 500 is a partial success: the running appliance did change and only writing the file
            // failed. Leaving the form dirty would claim the opposite.
            if (isApiError(error) && error.status === 500) {
                setPartialSaveMessage(error.message);
                await reseed();
                onSaved?.();
                toast.warning("Log settings applied but not saved", {
                    description: "They will be lost when the appliance restarts.",
                });
            }
            // Everything else changed nothing, so the rejected value stays in the field to be
            // corrected and the alert carries the server's reason.
        },
    });

    const submit = form.handleSubmit((values) => {
        if (values.logDirectory.trim() === "" && liveDirectory !== "") {
            setValuesAwaitingConfirmation(values);
            return;
        }
        saveMutation.mutate(values);
    });

    return {
        liveDirectory,
        partialSaveMessage,
        saveMutation,
        submit,
        valuesAwaitingConfirmation,
        setValuesAwaitingConfirmation,
    };
}
