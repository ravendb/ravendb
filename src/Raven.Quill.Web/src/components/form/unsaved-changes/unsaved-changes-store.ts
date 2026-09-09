import { create } from "zustand";
import { isInScope } from "@/components/form/unsaved-changes/unsaved-changes-scope";

/**
 * The dirty forms, as form id -> scope path. A single registry rather than a blocker per form,
 * because React Router consults only one blocker at a time.
 */
type UnsavedChangesState = {
    scopePathsByFormId: Record<string, string>;
    setUnsavedChanges: (formId: string, scopePath: string, hasUnsavedChanges: boolean) => void;
    clearUnsavedChanges: (formId: string) => void;
};

function withoutForm(scopePathsByFormId: Record<string, string>, formId: string) {
    if (!(formId in scopePathsByFormId)) {
        return null;
    }

    const remaining = { ...scopePathsByFormId };
    delete remaining[formId];
    return remaining;
}

export const useUnsavedChangesStore = create<UnsavedChangesState>((set) => ({
    scopePathsByFormId: {},
    setUnsavedChanges: (formId, scopePath, hasUnsavedChanges) =>
        set((state) => {
            if (!hasUnsavedChanges) {
                const remaining = withoutForm(state.scopePathsByFormId, formId);
                return remaining ? { scopePathsByFormId: remaining } : state;
            }

            if (state.scopePathsByFormId[formId] === scopePath) {
                return state;
            }

            return { scopePathsByFormId: { ...state.scopePathsByFormId, [formId]: scopePath } };
        }),
    clearUnsavedChanges: (formId) =>
        set((state) => {
            const remaining = withoutForm(state.scopePathsByFormId, formId);
            return remaining ? { scopePathsByFormId: remaining } : state;
        }),
}));

export function selectHasUnsavedChanges(state: UnsavedChangesState) {
    return Object.keys(state.scopePathsByFormId).length > 0;
}

export function selectHasUnsavedChangesInScope(scopePath: string) {
    return (state: UnsavedChangesState) =>
        Object.values(state.scopePathsByFormId).some((formScopePath) => isInScope(formScopePath, scopePath));
}
