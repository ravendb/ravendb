import type { AuthStatusResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const authMocks = {
    status: (status: AuthStatusResponse = authAuthenticated) =>
        apiHttp.get("/api/auth/status", ({ response }) => response(200).json(status)),
    login: (result: AuthStatusResponse = authAuthenticated) =>
        apiHttp.post("/api/auth/login", ({ response }) => response(200).json(result)),
    loginInvalid: () => apiHttp.post("/api/auth/login", ({ response }) => response(401).json(authUnauthenticated)),
    logout: () => apiHttp.post("/api/auth/logout", ({ response }) => response(204).empty()),
};

export const authAuthenticated: AuthStatusResponse = { authenticated: true };
export const authUnauthenticated: AuthStatusResponse = { authenticated: false };
