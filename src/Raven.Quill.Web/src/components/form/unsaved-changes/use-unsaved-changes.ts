import { useEffect, useId } from "react";
import { useFormState, type FieldValues, type UseFormReturn } from "react-hook-form";
import { useUnsavedChangesScopePath } from "@/components/form/unsaved-changes/unsaved-changes-scope";
import { useUnsavedChangesStore } from "@/components/form/unsaved-changes/unsaved-changes-store";

export type UnsavedChangesHandle = {
    /** Pass to `GuardedSheet`/`GuardedDialog` when the form lives beside the overlay, where its scope lookup cannot see it. */
    hasUnsavedChanges: boolean;
    /** Call after a successful save: the saved values become the new baseline and the form unregisters synchronously. */
    markSaved: () => void;
};

function useRegisterUnsavedChanges(formId: string, hasUnsavedChanges: boolean) {
    const scopePath = useUnsavedChangesScopePath();

    useEffect(() => {
        useUnsavedChangesStore.getState().setUnsavedChanges(formId, scopePath, hasUnsavedChanges);
    }, [formId, scopePath, hasUnsavedChanges]);

    useEffect(() => () => useUnsavedChangesStore.getState().clearUnsavedChanges(formId), [formId]);
}

/** Guards drafts react-hook-form does not own. Prefer {@link useFormUnsavedChanges} for forms. */
export function useUnsavedChanges(hasUnsavedChanges: boolean) {
    const formId = useId();
    useRegisterUnsavedChanges(formId, hasUnsavedChanges);
}

/** Asks for confirmation before a dirty form is abandoned - by route change, overlay close, or page unload. */
export function useFormUnsavedChanges<TValues extends FieldValues>(
    form: UseFormReturn<TValues>,
    /** Pass the mutation's pending state when the save is mutation-driven: `isSubmitting` drops the moment
     *  `mutate` returns, long before the save has settled. */
    options: { isSaving?: boolean } = {},
): UnsavedChangesHandle {
    const formId = useId();
    const { isDirty, isSubmitting } = useFormState({ control: form.control });

    // A save that navigates on success must not prompt on its way out.
    const hasUnsavedChanges = isDirty && !isSubmitting && !options.isSaving;
    useRegisterUnsavedChanges(formId, hasUnsavedChanges);

    return {
        hasUnsavedChanges,
        markSaved: () => {
            form.reset(form.getValues());
            useUnsavedChangesStore.getState().clearUnsavedChanges(formId);
        },
    };
}
