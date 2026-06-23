import type { ReactNode } from "react";
import { Navigate } from "react-router";
import { useAuth } from "@/components/auth/auth-context";
import { AuthScreenLayout } from "@/components/auth/auth-screen-layout";
import { Spinner } from "@/components/shadcn/ui/spinner";

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
        return <Navigate to="/" replace />;
    }

    return children;
}

function AuthLoading() {
    return (
        <AuthScreenLayout>
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Spinner className="size-4" />
                Checking authentication…
            </div>
        </AuthScreenLayout>
    );
}
