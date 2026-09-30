import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Navigate } from "react-router";
import { api } from "@/api/api";
import { useAuth } from "@/components/auth/auth-context";
import { AuthScreenLayout } from "@/components/auth/auth-screen-layout";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { appRoutes } from "@/lib/app-routes";
import { Text } from "@/components/typography";

export function RequireAuth({ children }: { children: ReactNode }) {
    const { isAuthenticated, isLoading } = useAuth();

    if (isLoading) {
        return <AuthLoading />;
    }

    if (!isAuthenticated) {
        return <Navigate to="/login" replace />;
    }

    return children;
}

export function RedirectAuthenticated({ children }: { children: ReactNode }) {
    const { isAuthenticated, isLoading } = useAuth();

    if (isLoading) {
        return <AuthLoading />;
    }

    if (isAuthenticated) {
        return <RedirectToLandingPage />;
    }

    return children;
}

// Owns the post-login destination so it cannot race a navigate() in the login form:
// flipping isAuthenticated re-renders this guard, and any competing navigation would
// be overridden by the redirect below.
function RedirectToLandingPage() {
    const appsQuery = useQuery(api.queries.apps.list());

    if (appsQuery.isPending) {
        return <AuthLoading />;
    }

    // On a failed lookup data stays undefined and we fall back to the dashboard.
    return <Navigate to={appsQuery.data?.length === 0 ? appRoutes.addApp() : appRoutes.dashboard()} replace />;
}

function AuthLoading() {
    return (
        <AuthScreenLayout>
            <Text variant="muted" as="div" className="flex items-center gap-2">
                <Spinner className="size-4" />
                Checking authentication…
            </Text>
        </AuthScreenLayout>
    );
}
