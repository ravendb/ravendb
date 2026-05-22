import { queryOptions } from "@tanstack/react-query";
import type { ApiClient } from "@/api/http-client";

export type AuthStatus = {
    isAuthenticated: boolean;
};

export type LoginRequest = {
    apiKey: string;
};

export function createAuthService(client: ApiClient) {
    return {
        getStatus: () => client.get<AuthStatus>("/auth/status"),
        login: (request: LoginRequest) => {
            console.log("login", request);
            return Promise.resolve<AuthStatus>({ isAuthenticated: true });
            // TODO uncomment when API is ready
            // return client.post<AuthStatus>("/auth/login", request);
        },
    };
}

export type AuthService = ReturnType<typeof createAuthService>;

export function createAuthQueries(api: AuthService) {
    return {
        status: () =>
            queryOptions({
                queryKey: ["auth", "status"],
                queryFn: () => api.getStatus(),
            }),
    };
}

export type AuthQueries = ReturnType<typeof createAuthQueries>;
