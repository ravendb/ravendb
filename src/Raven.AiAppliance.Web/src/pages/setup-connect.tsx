import { Link } from "react-router";
import { ArrowLeft, Database } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";

export function SetupConnect() {
    return (
        <div className="flex min-h-full w-full items-start">
            <section className="w-full rounded-lg border bg-card p-6 text-card-foreground shadow-xs">
                <Button asChild variant="ghost" size="sm" className="mb-6 w-fit">
                    <Link to="/">
                        <ArrowLeft className="size-4" aria-hidden="true" />
                        Apps
                    </Link>
                </Button>

                <div className="flex max-w-2xl gap-4">
                    <div className="flex size-9 shrink-0 items-center justify-center rounded-md bg-accent text-accent-foreground">
                        <Database className="size-5" aria-hidden="true" />
                    </div>
                    <div>
                        <h2 className="text-base font-semibold tracking-normal">Wizard coming soon</h2>
                        <p className="mt-3 text-sm leading-6 text-muted-foreground">
                            This placeholder reserves the setup flow for connecting a new app.
                        </p>
                    </div>
                </div>
            </section>
        </div>
    );
}
