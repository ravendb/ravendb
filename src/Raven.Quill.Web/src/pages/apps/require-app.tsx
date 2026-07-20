import { useQuery } from "@tanstack/react-query";
import { Link, Outlet, useParams } from "react-router";
import { api } from "@/api/api";
import { isApiError } from "@/api/http-client";
import { PageErrorState } from "@/components/data/page-error-state";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { appRoutes } from "@/lib/app-routes";

export function RequireApp() {
    const { slug = "" } = useParams();
    const appQuery = useQuery({
        ...api.queries.apps.detail(slug),
        enabled: Boolean(slug),
    });

    // Gate the outlet until the app is confirmed to exist, so a deep link to an unknown
    // slug doesn't flash the child page (and its failing requests) before the 404 state.
    if (appQuery.isLoading) {
        return (
            <section className="flex min-h-full items-center justify-center py-16">
                <Spinner className="size-6 text-muted-foreground" />
            </section>
        );
    }

    if (isApiError(appQuery.error) && appQuery.error.status === 404) {
        return (
            <PageErrorState
                title="App not found"
                description={
                    <p>
                        There is no app named{" "}
                        <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">{slug}</code>. It may have been
                        deleted or renamed.
                    </p>
                }
            >
                <Button asChild>
                    <Link to={appRoutes.dashboard()}>Go to dashboard</Link>
                </Button>
            </PageErrorState>
        );
    }

    return <Outlet />;
}
