import { useIsMutating, useMutation } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";
import { toast } from "sonner";
import { api } from "@/api/api";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

const REMOVE_PLANNER_COLLECTION_KEY = ["planner", "removeCollection"];

export function useRemovePlannerCollection() {
    const { getValues } = useFormContext<AppFormData>();

    return useMutation({
        mutationKey: REMOVE_PLANNER_COLLECTION_KEY,
        mutationFn: async (collection: string) => {
            const conversationId = useSetupWizardStore.getState().plannerConversationId;

            if (conversationId === null) {
                throw new Error(`Could not remove ${collection}: the planner session is gone.`);
            }

            await api.services.setup.migrationEditCollection({
                slug: getValues("externalConnection").slug,
                conversationId,
                collection,
                config: null,
            });

            return conversationId;
        },
        onSuccess: (conversationId, collection) => {
            const store = useSetupWizardStore.getState();

            // A session started over meanwhile may hold a collection by the same name; it stays.
            if (store.plannerConversationId === conversationId) {
                store.removePlannerCollection(collection);
            }
        },
        onError: (error, collection) => toast.error(error.message || `Could not remove ${collection} from the plan.`),
    });
}

/** Each removal rewrites the stored plan, so the whole plan waits for one to finish before the next. */
export function useIsRemovingPlannerCollection(): boolean {
    return useIsMutating({ mutationKey: REMOVE_PLANNER_COLLECTION_KEY }) > 0;
}
