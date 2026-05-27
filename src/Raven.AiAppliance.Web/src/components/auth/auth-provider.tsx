import type { ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { RedeemLicenseRequest } from "@/api/generated/server-api";
import { AuthContext } from "@/components/auth/auth-context";

export function AuthProvider({ children }: { children: ReactNode }) {
    const queryClient = useQueryClient();
    const authStatusQuery = api.queries.bootstrap.status();
    const statusQuery = useQuery(authStatusQuery);
    const isAuthenticated = statusQuery.data?.state === "Ready";

    async function login(request: RedeemLicenseRequest) {
        const status = await api.services.bootstrap.redeemLicense(request);
        queryClient.setQueryData(authStatusQuery.queryKey, status);
        return status;
    }

    return (
        <AuthContext.Provider
            value={{
                isAuthenticated,
                isLoading: statusQuery.isPending,
                login,
            }}
        >
            {children}
        </AuthContext.Provider>
    );
}
