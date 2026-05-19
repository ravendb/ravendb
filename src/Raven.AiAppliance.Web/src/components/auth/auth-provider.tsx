import { useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { LoginRequest } from "@/api/authApi";
import { AuthContext } from "@/components/auth/auth-context";

export function AuthProvider({ children }: { children: ReactNode }) {
  const [isLoggedIn, setIsLoggedIn] = useState<boolean | null>(null);
  const statusQuery = useQuery(api.queries.auth.status());
  const isAuthenticated =
    isLoggedIn ?? statusQuery.data?.isAuthenticated ?? false;

  async function login(request: LoginRequest) {
    console.log("login", request);
    setIsLoggedIn(true);
    return true;
    // TODO uncomment when API is ready
    // const status = await api.services.auth.login(request);
    // setIsLoggedIn(status.isAuthenticated);
    // return status.isAuthenticated;
  }

  return (
    <AuthContext.Provider
      value={{
        isAuthenticated,
        isLoading: statusQuery.isLoading,
        login,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}
