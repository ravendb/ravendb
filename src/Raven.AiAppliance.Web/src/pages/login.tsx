import { zodResolver } from "@hookform/resolvers/zod";
import { LogIn } from "lucide-react";
import { useForm } from "react-hook-form";
import { useNavigate } from "react-router";
import { z } from "zod";
import { useAuth } from "@/components/auth/auth-context";
import { FormInput } from "@/components/form/forn-input";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

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
    <main className="grid min-h-svh place-items-center bg-background px-4 py-8 text-foreground">
      <section className="w-full max-w-sm rounded-lg border bg-card p-6 text-card-foreground shadow-xs">
        <div className="flex items-center gap-3">
          <div className="flex size-9 items-center justify-center rounded-md bg-primary text-primary-foreground">
            <LogIn className="size-4" aria-hidden="true" />
          </div>
          <div>
            <h1 className="text-sm font-semibold">RavenDB AI Appliance</h1>
            <p className="text-xs text-muted-foreground">Sign in</p>
          </div>
        </div>

        <form className="mt-6 space-y-4" onSubmit={handleSubmit(handleLogin)}>
          <FormInput
            control={control}
            name="apiKey"
            label="API key"
            type="password"
          />
          <Button className="w-full" disabled={isSubmitting}>
            {isSubmitting ? "Signing in..." : "Sign in"}
          </Button>
        </form>
      </section>
    </main>
  );
}

const loginSchema = z.object({
  apiKey: z.string().trim().min(1, "API key is required."),
});

type LoginFormValues = z.infer<typeof loginSchema>;
