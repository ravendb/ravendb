import { Link } from "react-router";
import { ArrowRight, Plus } from "lucide-react";
import { Button } from "@/components/ui/button";

export function DashboardHome() {
  return (
    <div className="mx-auto flex min-h-[calc(100svh-7rem)] w-full max-w-5xl flex-col gap-6">
      <section className="rounded-lg border bg-card p-6 text-card-foreground shadow-xs">
        <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
          <div className="space-y-2">
            <p className="text-sm font-medium text-primary">Apps</p>
            <h1 className="text-2xl font-semibold tracking-normal">
              No applications yet
            </h1>
            <p className="max-w-2xl text-sm leading-6 text-muted-foreground">
              Add an application to connect a source database, prepare CDC, and
              provision the first AI agent when the wizard is ready.
            </p>
          </div>
          <Button asChild size="lg" className="w-full md:w-auto">
            <Link to="/setup/connect">
              <Plus className="size-4" aria-hidden="true" />
              Add your first application
              <ArrowRight className="size-4" aria-hidden="true" />
            </Link>
          </Button>
        </div>
      </section>

      <section className="grid gap-3 md:grid-cols-3">
        {["CDC status", "Agent health", "Channels"].map((label) => (
          <div
            key={label}
            className="rounded-lg border bg-card p-4 text-card-foreground shadow-xs"
          >
            <p className="text-sm font-medium">{label}</p>
            <p className="mt-2 text-sm text-muted-foreground">Empty</p>
          </div>
        ))}
      </section>
    </div>
  );
}
