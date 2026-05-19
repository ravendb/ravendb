import { Link } from "react-router";
import { ArrowLeft } from "lucide-react";
import { Button } from "@/components/ui/button";

export function SetupConnect() {
  return (
    <div className="min-h-svh bg-background px-4 py-6 text-foreground">
      <div className="mx-auto flex w-full max-w-3xl flex-col gap-6">
        <Button asChild variant="ghost" className="w-fit">
          <Link to="/">
            <ArrowLeft className="size-4" aria-hidden="true" />
            Dashboard
          </Link>
        </Button>

        <section className="rounded-lg border bg-card p-6 text-card-foreground shadow-xs">
          <p className="text-sm font-medium text-primary">Setup</p>
          <h1 className="mt-2 text-2xl font-semibold tracking-normal">
            Wizard coming soon
          </h1>
          <p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground">
            This placeholder reserves the `/setup/connect` route for the
            application setup wizard.
          </p>
        </section>
      </div>
    </div>
  );
}
