import { createContext, useContext } from "react";
import type { BootstrapStatusResponse, RedeemLicenseRequest } from "@/api/generated/server-api";

export type AuthContextValue = {
    isAuthenticated: boolean;
    isLoading: boolean;
    login: (request: RedeemLicenseRequest) => Promise<BootstrapStatusResponse>;
};

export const AuthContext = createContext<AuthContextValue | null>(null);

export function useAuth() {
    const context = useContext(AuthContext);

    if (!context) {
        throw new Error("useAuth must be used within AuthProvider.");
    }

    return context;
}
