import { useMutation } from "@tanstack/react-query";
import { CircleCheck, PlugZap } from "lucide-react";
import { useFormContext, useWatch, type FieldPath } from "react-hook-form";
import { WizardErrorAlert } from "@/components/form/wizard/wizard-error-alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    computeConnectKey,
    getConnectionError,
    isConnectionVerified,
    testConnection,
} from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";

const CONNECTION_FIELDS = [
    "externalConnection.provider",
    "externalConnection.connectionString",
    "externalConnection.slug",
] as const satisfies FieldPath<AppFormData>[];

export function TestConnectionButton({ disabled }: { disabled: boolean }) {
    const { control, getValues, trigger } = useFormContext<AppFormData>();
    const connectionAttempt = useSetupWizardStore((state) => state.connectionAttempt);

    // The key must be derived from the watched values (not getValues), otherwise React Compiler
    // memoizes it against the stable getValues reference and it never reflects form changes.
    const [provider, connectionString, slug] = useWatch({ control, name: CONNECTION_FIELDS });
    const connectKey = computeConnectKey({ provider, connectionString, slug });
    const isVerified = isConnectionVerified(connectionAttempt, connectKey);
    const error = getConnectionError(connectionAttempt, connectKey);

    // testConnection stores its outcome, so the mutation only drives the pending state.
    const { mutate, isPending } = useMutation({
        mutationFn: () => testConnection(getValues("externalConnection")),
    });

    const handleTest = async () => {
        if (await trigger([...CONNECTION_FIELDS])) {
            mutate();
        }
    };

    return (
        <div className="grid gap-3">
            <div className="flex">
                <Button
                    type="button"
                    variant="outline"
                    onClick={handleTest}
                    disabled={disabled || isPending || isVerified}
                    className={cn(isVerified && "border-success/40 text-success disabled:opacity-100")}
                >
                    {isPending ? <Spinner /> : isVerified ? <CircleCheck /> : <PlugZap />}
                    {isVerified ? "Connection verified" : "Test connection"}
                </Button>
            </div>
            {error && <WizardErrorAlert error={error} />}
        </div>
    );
}
