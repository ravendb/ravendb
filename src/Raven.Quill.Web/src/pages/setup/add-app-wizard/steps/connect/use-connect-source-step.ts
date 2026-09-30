import { api } from "@/api/api";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type { WizardProgress } from "@/components/form/wizard/form-wizard";
import { toError, toWizardStepError, WizardHandledError } from "@/components/form/wizard/wizard-step-error";
import { useSetupWizardStore, type ConnectionAttempt } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { resolveConnectionString } from "@/pages/setup/add-app-wizard/connection-string";
import { getTableKey, isTableSupported } from "@/pages/setup/add-app-wizard/discover-utils";
import { discoverTables } from "@/pages/setup/add-app-wizard/steps/verify/use-discover-tables";
import { useFormContext } from "react-hook-form";

export type ConnectSourceInput = Pick<
    AppFormData["externalConnection"],
    "provider" | "mode" | "fields" | "connectionString" | "slug"
>;

/** Identifies the source database, and with it the discovered schema and any mapping built from it. */
export function computeSourceKey(connection: Omit<ConnectSourceInput, "slug">): string {
    return JSON.stringify({
        provider: connection.provider,
        connectionString: resolveConnectionString(connection),
    });
}

/**
 * Identifies what the server holds for this wizard. Its state document is keyed by slug, so a new slug
 * means an empty document: everything the wizard had the server do must run again under that slug.
 */
export function computeConnectKey(connection: ConnectSourceInput): string {
    return JSON.stringify({
        sourceKey: computeSourceKey(connection),
        slug: connection.slug,
    });
}

export function isConnectionVerified(attempt: ConnectionAttempt | null, connectKey: string): boolean {
    return attempt?.key === connectKey && attempt.error === null;
}

export function getConnectionError(attempt: ConnectionAttempt | null, connectKey: string): Error | null {
    return attempt?.key === connectKey ? attempt.error : null;
}

/**
 * Records the outcome for the inputs it ran with, so the connect step renders the single result the
 * operator sees. Failures surface as WizardHandledError to keep the wizard from alerting about them again.
 */
export async function testConnection(connection: ConnectSourceInput): Promise<void> {
    const key = computeConnectKey(connection);
    const { setConnectionAttempt } = useSetupWizardStore.getState();

    try {
        const connectResult = await api.services.setup.connect({
            connectionString: resolveConnectionString(connection),
            provider: connection.provider,
            slug: connection.slug,
        });

        if (!connectResult.success) {
            throw toWizardStepError(connectResult.errors, "Connection failed.");
        }
    } catch (error) {
        setConnectionAttempt({ key, error: toError(error) });
        throw new WizardHandledError(error);
    }

    setConnectionAttempt({ key, error: null });
}

export function useConnectSourceStep() {
    const { getValues, setValue } = useFormContext<AppFormData>();
    const setDiscoverResult = useSetupWizardStore((state) => state.setDiscoverResult);

    return async (progress: WizardProgress) => {
        const store = useSetupWizardStore.getState();
        const formValues = getValues("externalConnection");
        const connectKey = computeConnectKey(formValues);

        if (connectKey === store.connectKey) {
            return;
        }

        if (!isConnectionVerified(store.connectionAttempt, connectKey)) {
            progress.report("Testing connection...");
            await testConnection(formValues);
        }

        progress.report("Discovering tables...");
        const schemas = store.discoverSchemas;
        const discoverResult = await discoverTables(formValues, schemas, formValues.slug);

        setDiscoverResult(discoverResult, schemas);

        // Tables selected under the previous connection may not exist in the new schema; keep
        // only those still verified so the verify step never seeds a stale selection.
        const verifiedKeys = new Set(
            discoverResult.tables
                .filter((table) => isTableSupported(discoverResult, table))
                .map((table) => getTableKey(table)),
        );
        setValue(
            "verifySchema.tables",
            getValues("verifySchema.tables").filter((table) => verifiedKeys.has(getTableKey(table))),
        );

        store.setConnectKey(connectKey);
    };
}
