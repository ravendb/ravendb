import { isRouteErrorResponse, Link, useRouteError } from "react-router";
import { AuthScreenLayout } from "@/components/auth/auth-screen-layout";
import { PageErrorState } from "@/components/data/page-error-state";
import { Button } from "@/components/shadcn/ui/button";
import { appRoutes } from "@/lib/app-routes";

export function RouteErrorBoundary() {
    const error = useRouteError();
    const isNotFound = isRouteErrorResponse(error) && error.status === 404;

    if (isNotFound) {
        return (
            <AuthScreenLayout background="simple">
                <PageErrorState
                    code="404"
                    title="Page not found"
                    description={<p>The page you&apos;re looking for doesn&apos;t exist or has been moved.</p>}
                >
                    <Button asChild>
                        <Link to={appRoutes.dashboard()}>Go to dashboard</Link>
                    </Button>
                </PageErrorState>
            </AuthScreenLayout>
        );
    }

    return (
        <AuthScreenLayout background="simple">
            <PageErrorState title="Something went wrong" description={<p>{getErrorMessage(error)}</p>}>
                <Button variant="outline" asChild>
                    <Link to={appRoutes.dashboard()}>Go to dashboard</Link>
                </Button>
            </PageErrorState>
        </AuthScreenLayout>
    );
}

function getErrorMessage(error: unknown) {
    if (isRouteErrorResponse(error)) {
        return `${error.status} ${error.statusText}`.trim();
    }

    if (error instanceof Error && error.message) {
        return error.message;
    }

    return "An unexpected error occurred.";
}
