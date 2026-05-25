import { zodResolver } from "@hookform/resolvers/zod";
import { useQuery } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";
import { useRef, useState } from "react";
import { useForm } from "react-hook-form";
import { useNavigate } from "react-router";
import { toast } from "sonner";
import { z } from "zod";
import { api } from "@/api/api";
import type { BootstrapPhase } from "@/api/bootstrap-service";
import { FormInput } from "@/components/form/form-input";
import { Button } from "@/components/shadcn/ui/button";
import { useAuth } from "@/components/auth/auth-context";

export function Login() {
    const { login } = useAuth();
    const navigate = useNavigate();
    const bootstrapStatusQuery = api.queries.bootstrap.status();
    const activationStartedAtRef = useRef<number | null>(null);
    const [activationTimedOut, setActivationTimedOut] = useState(false);
    const statusQuery = useQuery({
        ...bootstrapStatusQuery,
        refetchInterval: (query) => {
            if (!isActivationPending(query.state.data?.state)) {
                activationStartedAtRef.current = null;
                return false;
            }

            activationStartedAtRef.current ??= query.state.dataUpdatedAt;

            const lastStatusCheckAt = Math.max(query.state.dataUpdatedAt, query.state.errorUpdatedAt);
            if (lastStatusCheckAt - activationStartedAtRef.current >= ACTIVATION_TIMEOUT_MS) {
                setActivationTimedOut(true);
                return false;
            }

            return activationTimedOut ? false : ACTIVATION_POLL_INTERVAL_MS;
        },
    });
    const isActivationWaiting = isActivationPending(statusQuery.data?.state);
    const {
        control,
        formState: { isSubmitting },
        handleSubmit,
    } = useForm<LoginFormValues>({
        defaultValues: {
            licenseKey: "",
        },
        resolver: zodResolver(loginSchema),
    });

    async function handleLogin(values: LoginFormValues) {
        try {
            const status = await login(values);
            if (status.state === "ready") {
                navigate("/", {
                    replace: true,
                });
                return;
            }

            if (isActivationPending(status.state)) {
                setActivationTimedOut(false);
                return;
            }

            if (status.state === "needs-activation") {
                toast.error("Activation could not be started. Check the license key and try again.");
                return;
            }
        } catch {
            toast.error("Sign in failed. Please try again later.");
        }
    }

    return (
        <main className="flex min-h-svh items-center justify-center px-4 py-8">
            <div className="w-full max-w-lg">
                <div className="mb-5 flex items-center justify-center gap-2">
                    <div className="flex size-6 items-center justify-center rounded-lg bg-primary" />
                    <span className="text-sm font-medium">RavenDB Appliance</span>
                </div>

                <section className="rounded-xl border bg-card px-6 py-7">
                    <div className="text-center">
                        <h1 className="text-xl font-semibold">Activate dashboard</h1>
                        <p className="mt-3 text-sm text-muted-foreground">Enter the license key for this appliance.</p>
                    </div>

                    {isActivationWaiting ? (
                        <ActivationWaiting
                            timedOut={activationTimedOut}
                            onRetry={() => {
                                activationStartedAtRef.current = null;
                                setActivationTimedOut(false);
                                void statusQuery.refetch();
                            }}
                        />
                    ) : (
                        <form className="mt-7 space-y-5" onSubmit={handleSubmit(handleLogin)}>
                            <FormInput control={control} name="licenseKey" label="License key" type="password" />

                            <Button className="w-full" disabled={isSubmitting}>
                                {isSubmitting ? "Activating..." : "Continue"}
                            </Button>
                        </form>
                    )}
                </section>
            </div>
        </main>
    );
}

const loginSchema = z.object({
    licenseKey: z.string().trim().min(1, "License key is required."),
});

type LoginFormValues = z.infer<typeof loginSchema>;

const ACTIVATION_POLL_INTERVAL_MS = 5_000;
const ACTIVATION_TIMEOUT_MS = 120_000;

function isActivationPending(state: BootstrapPhase | undefined) {
    return state === "redeeming" || state === "restarting";
}

function ActivationWaiting({ timedOut, onRetry }: { timedOut: boolean; onRetry: () => void }) {
    return (
        <div className="mt-7 space-y-5 text-center">
            <Loader2 className="mx-auto size-8 animate-spin text-primary" aria-hidden="true" />
            <div className="space-y-2">
                <h2 className="text-base font-semibold">
                    {timedOut ? "Activation is taking longer than expected" : "Restarting server"}
                </h2>
                <p className="text-sm text-muted-foreground">
                    {timedOut
                        ? "The server did not report readiness within 120 seconds. Check the status again in a moment."
                        : "Activation was accepted. Wait up to 120 seconds while the server restarts."}
                </p>
            </div>
            {timedOut && (
                <Button className="w-full" onClick={onRetry}>
                    Check again
                </Button>
            )}
        </div>
    );
}
