import { createContext, useContext } from "react";
import type { LoginRequest } from "@/api/auth-service";

export type AuthContextValue = {
    isAuthenticated: boolean;
    isLoading: boolean;
    login: (request: LoginRequest) => Promise<boolean>;
};

export const AuthContext = createContext<AuthContextValue | null>(null);

export function useAuth() {
    const context = useContext(AuthContext);

    if (!context) {
        throw new Error("useAuth must be used within AuthProvider.");
    }

    return context;
}
