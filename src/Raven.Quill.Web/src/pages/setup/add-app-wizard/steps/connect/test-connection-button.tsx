import { useMutation } from "@tanstack/react-query";
import { CircleCheck, PlugZap } from "lucide-react";
import { useFormContext, useWatch, type FieldPath } from "react-hook-form";
import { WizardErrorAlert } from "@/components/form/wizard/wizard-error-alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { computeConnectKey, connectToSource } from "@/pages/setup/add-app-wizard/steps/connect/use-connect-source-step";

const CONNECTION_FIELDS = [
    "externalConnection.provider",
    "externalConnection.connectionString",
    "externalConnection.slug",
] as const satisfies FieldPath<AppFormData>[];

export function TestConnectionButton({ disabled }: { disabled: boolean }) {
    const { control, getValues, trigger } = useFormContext<AppFormData>();
    const testedConnectKey = useSetupWizardStore((state) => state.testedConnectKey);
    const setTestedConnectKey = useSetupWizardStore((state) => state.setTestedConnectKey);

    // The key must be derived from the watched values (not getValues), otherwise React Compiler
    // memoizes it against the stable getValues reference and it never reflects form changes.
    const [provider, connectionString, slug] = useWatch({ control, name: CONNECTION_FIELDS });
    const isTested = computeConnectKey({ provider, connectionString, slug }) === testedConnectKey;

    const { mutate, isPending, error } = useMutation({
        mutationFn: async () => {
            const connection = getValues("externalConnection");
            await connectToSource(connection);
            return computeConnectKey(connection);
        },
        onSuccess: setTestedConnectKey,
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
                    disabled={disabled || isPending || isTested}
                    className={cn(isTested && "border-success/40 text-success disabled:opacity-100")}
                >
                    {isPending ? <Spinner /> : isTested ? <CircleCheck /> : <PlugZap />}
                    {isTested ? "Connection verified" : "Test connection"}
                </Button>
            </div>
            {error && !isTested && <WizardErrorAlert error={error} />}
        </div>
    );
}
