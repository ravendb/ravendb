import { zodResolver } from "@hookform/resolvers/zod";
import { useState } from "react";
import { useForm } from "react-hook-form";
import { CircleAlert } from "lucide-react";
import { z } from "zod";
import { isApiError } from "@/api/http-client";
import { useAuth } from "@/components/auth/auth-context";
import { AuthScreenLayout } from "@/components/auth/auth-screen-layout";
import { FormInput } from "@/components/form/form-input";
import { Alert, AlertTitle } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Heading, Text } from "@/components/typography";

const INVALID_KEY_MESSAGE = "That API key wasn't accepted. Double-check it and try again.";
const SIGN_IN_ERROR_MESSAGE = "We couldn't sign you in. Please try again in a moment.";

export function Login() {
    const { login } = useAuth();
    const [formError, setFormError] = useState<string | null>(null);
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
        setFormError(null);

        try {
            const result = await login(values.apiKey);

            // On success RedirectAuthenticated takes over and navigates to the landing page.
            if (!result.authenticated) {
                setFormError(INVALID_KEY_MESSAGE);
            }
        } catch (error) {
            setFormError(isApiError(error) && error.status === 401 ? INVALID_KEY_MESSAGE : SIGN_IN_ERROR_MESSAGE);
        }
    }

    return (
        <AuthScreenLayout>
            <section className="w-full rounded-xl border bg-card p-6 shadow-sm">
                <header className="space-y-1.5 text-center">
                    <Heading as="h1" variant="title">
                        Sign in
                    </Heading>
                    <Text variant="muted">Enter the Dashboard API key to manage Quill.</Text>
                </header>

                {formError && (
                    <Alert variant="destructive" className="mt-5 border-destructive/30 bg-destructive/5">
                        <CircleAlert />
                        <AlertTitle>{formError}</AlertTitle>
                    </Alert>
                )}

                <form className="mt-5 space-y-4" onSubmit={handleSubmit(handleLogin)} noValidate>
                    <FormInput
                        control={control}
                        name="apiKey"
                        label="Dashboard API key"
                        type="password"
                        placeholder="QUILLDASH-..."
                        autoComplete="off"
                        autoFocus
                        spellCheck={false}
                    />

                    <Button className="w-full" disabled={isSubmitting} type="submit">
                        {isSubmitting ? (
                            <>
                                <Spinner />
                                Signing in…
                            </>
                        ) : (
                            "Continue"
                        )}
                    </Button>
                </form>
            </section>

            <Text variant="caption" className="mt-6 max-w-sm text-center">
                The Dashboard API key was issued when Quill was provisioned.
            </Text>
        </AuthScreenLayout>
    );
}

const loginSchema = z.object({
    apiKey: z.string().trim().min(1, "API key is required."),
});

type LoginFormValues = z.infer<typeof loginSchema>;
