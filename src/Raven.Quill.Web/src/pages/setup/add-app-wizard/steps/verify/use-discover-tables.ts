import { useMutation } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";
import { toast } from "sonner";
import { api } from "@/api/api";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

/** Trims entries, drops empties, and dedupes the user-entered schema list. */
export function normalizeDiscoverSchemas(schemas: string[] | undefined): string[] {
    return [...new Set((schemas ?? []).map((schema) => schema.trim()).filter(Boolean))];
}

/** Discovers source tables. An empty schema list means the connection's default schema. */
export function discoverTables(
    connection: Pick<AppFormData["externalConnection"], "provider" | "connectionString">,
    schemas: string[],
    slug: string,
) {
    return api.services.setup.discover({
        provider: connection.provider,
        connectionString: connection.connectionString,
        schemas: schemas.length > 0 ? schemas : null,
        slug,
    });
}

/** Re-runs discovery from the verify step (e.g. after the schema list changed). */
export function useDiscoverTablesMutation() {
    const { getValues } = useFormContext<AppFormData>();
    const setDiscoverResult = useSetupWizardStore((state) => state.setDiscoverResult);

    return useMutation({
        mutationFn: (schemas: string[]) => {
            const connection = getValues("externalConnection");
            return discoverTables(connection, schemas, connection.slug);
        },
        onSuccess: (result, schemas) => setDiscoverResult(result, schemas),
        onError: (error) => {
            toast.error(error instanceof Error ? error.message : "Could not discover tables.");
        },
    });
}
