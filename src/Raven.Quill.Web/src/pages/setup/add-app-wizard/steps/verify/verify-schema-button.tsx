import { useVerifySchemaCdcState } from "@/pages/setup/add-app-wizard/steps/verify/use-verify-schema-cdc";
import { VerifyCdcButton } from "@/pages/setup/add-app-wizard/steps/verify/verify-cdc-button";

const VERIFY_SCHEMA_LABELS = {
    idle: "Verify schema",
    verifying: "Verifying schema...",
    verified: "Schema verified",
};

/**
 * The CDC dry run for the verify step's table selection. Lives in the selection overlay:
 * verifying is only meaningful once at least one table is selected.
 */
export function VerifySchemaButton({ disabled }: { disabled: boolean }) {
    const state = useVerifySchemaCdcState();

    return <VerifyCdcButton disabled={disabled} state={state} labels={VERIFY_SCHEMA_LABELS} />;
}
