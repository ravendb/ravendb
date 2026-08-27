import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useFormState, useWatch } from "react-hook-form";
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

    // Restores just this section's fields from the saved theme. The form is seeded through `values`, so
    // each field's default already is the saved value and resetField needs no defaultValue of its own -
    // and passing one leaves the field's entry in dirtyFields behind, which kept each section's undo
    // button on screen after the undo had happened.
    const resetSection = (paths: readonly (keyof WidgetThemeFormData)[]) => {
        for (const path of paths) {
            form.resetField(path);
        }
    };

    const previewTheme = toPreviewTheme(useWatch({ control: form.control }), savedTheme);

    return { form, isDirty, previewTheme, saveTheme, discardChanges, resetSection };
}
