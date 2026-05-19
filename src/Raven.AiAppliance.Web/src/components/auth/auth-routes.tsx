import type { ReactNode } from "react";
import { Navigate } from "react-router";
import { useAuth } from "@/components/auth/auth-context";

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
    <div className="grid min-h-svh place-items-center bg-background px-4 text-sm text-muted-foreground">
      Checking authentication...
    </div>
  );
}
