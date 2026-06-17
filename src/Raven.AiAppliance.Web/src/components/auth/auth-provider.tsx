import type { ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
import { AuthContext } from "@/components/auth/auth-context";

const authStatusQueryKey = ["auth", "status"];

export function AuthProvider({ children }: { children: ReactNode }) {
    const queryClient = useQueryClient();
    const statusQuery = useQuery({
        queryKey: authStatusQueryKey,
        queryFn: () => api.services.auth.status(),
    });
    const isAuthenticated = statusQuery.data?.authenticated === true;

    async function login(apiKey: string) {
        const status = await api.services.auth.login({ apiKey });
        queryClient.setQueryData(authStatusQueryKey, status);
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
