import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useNavigate } from "react-router";
import { toast } from "sonner";
import { z } from "zod";
import { FormInput } from "@/components/form/form-input";
import { Button } from "@/components/shadcn/ui/button";
import { useAuth } from "@/components/auth/auth-context";

export function Login() {
    const { login } = useAuth();
    const navigate = useNavigate();
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
            const isAuthenticated = await login(values);
            if (!isAuthenticated) {
                toast.error("Activation is not ready yet.");
                return;
            }

            navigate("/", {
                replace: true,
            });
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

                    <form className="mt-7 space-y-5" onSubmit={handleSubmit(handleLogin)}>
                        <FormInput control={control} name="licenseKey" label="License key" type="password" />

                        <Button className="w-full" disabled={isSubmitting}>
                            {isSubmitting ? "Activating..." : "Continue"}
                        </Button>
                    </form>
                </section>
            </div>
        </main>
    );
}

const loginSchema = z.object({
    licenseKey: z.string().trim().min(1, "License key is required."),
});

type LoginFormValues = z.infer<typeof loginSchema>;
