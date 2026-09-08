import { createContext, useContext } from "react";

export type LoginResult = {
    authenticated: boolean;
};

export type AuthContextValue = {
    isAuthenticated: boolean;
    isLoading: boolean;
    login: (apiKey: string) => Promise<LoginResult>;
    logout: () => Promise<void>;
};

export const AuthContext = createContext<AuthContextValue | null>(null);

export function useAuth() {
    const context = useContext(AuthContext);

    if (!context) {
        throw new Error("useAuth must be used within AuthProvider.");
    }

    return context;
}
