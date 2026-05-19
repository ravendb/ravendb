import { queryOptions } from "@tanstack/react-query";
import type { ApiClient } from "@/api/httpClient";

export type AuthStatus = {
  isAuthenticated: boolean;
};

export type LoginRequest = {
  apiKey: string;
};

export function createAuthApi(client: ApiClient) {
  return {
    getStatus: () => client.get<AuthStatus>("/auth/status"),
    login: (request: LoginRequest) =>
      client.post<AuthStatus>("/auth/login", request),
  };
}

export type AuthApi = ReturnType<typeof createAuthApi>;

const authKeys = {
  all: ["auth"],
  status: () => [...authKeys.all, "status"],
} as const;

export function createAuthQueries(api: AuthApi) {
  return {
    status: () =>
      queryOptions({
        queryKey: authKeys.status(),
        queryFn: () => api.getStatus(),
      }),
  };
}

export type AuthQueries = ReturnType<typeof createAuthQueries>;
