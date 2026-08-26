import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useFormState, useWatch, type FieldPath } from "react-hook-form";
import type { WidgetTheme } from "@/api/generated/server-api";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import {
    toFormData,
    toPreviewTheme,
    widgetThemeSchema,
    type WidgetThemeFormData,
    type WidgetThemeFormOutput,
} from "@/pages/apps/channels/web-widget-theme-schema";

type UseThemeEditorFormOptions = {
    savedTheme: WidgetTheme;
    /** Resolves once the theme has reached the server; rejects so a failed save leaves the form dirty. */
    onSave: (theme: WidgetTheme | null) => Promise<unknown>;
};

/** Owns the theme form, its dirty/preview state and the save/discard/reset actions built on top of it. */
export function useThemeEditorForm({ savedTheme, onSave }: UseThemeEditorFormOptions) {
    const form = useForm<WidgetThemeFormData, unknown, WidgetThemeFormOutput>({
        resolver: zodResolver(widgetThemeSchema),
        // The app default seeds the form while a widget follows it, so switching to an own theme starts from
        // what the operator is already looking at instead of from the built-in.
        values: toFormData(savedTheme),
        // The re-seed is load-bearing - Follow app default saves null and the editor has to pick up the app
        // default that comes back - but it must not overwrite fields the operator has already edited.
        resetOptions: { keepDirtyValues: true },
    });

    const unsavedChanges = useFormUnsavedChanges(form);
    const { isDirty } = useFormState({ control: form.control });

    const saveTheme = async (themeToSave: WidgetTheme | null) => {
        try {
            await onSave(themeToSave);
        } catch {
            // The mutation owns the error UI. Leaving the form dirty keeps the guard armed until the
            // operator's work has actually reached the server.
            return;
        }
        unsavedChanges.markSaved();
    };

    // useForm's resetOptions default every reset() call to keepDirtyValues: true, which is
    // exactly backwards here - Discard exists to overwrite the dirty fields, so it opts back out.
    const discardChanges = () => form.reset(toFormData(savedTheme), { keepDirtyValues: false });

    // Restores just this section's fields from the saved theme. keepDirtyValues is opted out of for the
    // same reason Discard opts out: the point is to overwrite the fields the operator edited.
    const resetSection = (paths: readonly FieldPath<WidgetThemeFormData>[]) => {
        const savedValues = toFormData(savedTheme);
        for (const path of paths) {
            form.resetField(path, { defaultValue: savedValues[path as keyof WidgetThemeFormData] });
        }
    };

    const previewTheme = toPreviewTheme(useWatch({ control: form.control }), savedTheme);

    return { form, isDirty, previewTheme, saveTheme, discardChanges, resetSection };
}
