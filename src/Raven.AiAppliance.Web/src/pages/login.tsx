import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { useNavigate } from "react-router";
import { z } from "zod";
import { useAuth } from "@/components/auth/auth-context";
import { Button } from "@/components/shadcn/ui/button";
import { toast } from "sonner";
import { FormInput } from "@/components/form/form-input";

export function Login() {
    const { login } = useAuth();
    const navigate = useNavigate();
    const {
        control,
        formState: { isSubmitting },
        handleSubmit,
    } = useForm<LoginFormValues>({
        defaultValues: {
            apiKey: "",
        },
        resolver: zodResolver(loginSchema),
    });

    async function handleLogin(values: LoginFormValues) {
        try {
            const isAuthenticated = await login(values);
            if (!isAuthenticated) {
                toast.error("Sign in failed. Please check your API key and try again.");
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
                        <h1 className="text-xl font-semibold">Enter your dashboard API Key</h1>
                        <p className="mt-3 text-sm text-muted-foreground">We sent it to your email</p>
                    </div>

                    <form className="mt-7 space-y-5" onSubmit={handleSubmit(handleLogin)}>
                        <FormInput control={control} name="apiKey" label="API key" type="password" />

                        <Button className="w-full" disabled={isSubmitting}>
                            {isSubmitting ? "Signing in..." : "Continue"}
                        </Button>
                    </form>

                    <div className="mt-7 border-t pt-6">
                        <p className="text-sm text-muted-foreground">Lost your key? Run on the host:</p>
                        <code className="mt-3 block rounded-md border bg-muted px-3 py-3 font-mono text-xs text-foreground">
                            docker exec bridge bridge regen-api-key
                        </code>
                    </div>
                </section>
            </div>
        </main>
    );
}

const loginSchema = z.object({
    apiKey: z.string().trim().min(1, "API key is required."),
});

type LoginFormValues = z.infer<typeof loginSchema>;
