import { createContext, useContext } from "react";
import type { RedeemLicenseRequest, RedeemLicenseResponse } from "@/api/bootstrap-service";

export type AuthContextValue = {
    isAuthenticated: boolean;
    isLoading: boolean;
    login: (request: RedeemLicenseRequest) => Promise<RedeemLicenseResponse>;
};

export const AuthContext = createContext<AuthContextValue | null>(null);

export function useAuth() {
    const context = useContext(AuthContext);

    if (!context) {
        throw new Error("useAuth must be used within AuthProvider.");
    }

    return context;
}
