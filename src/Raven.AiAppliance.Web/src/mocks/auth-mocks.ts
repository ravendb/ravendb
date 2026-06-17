import type { AuthStatusResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const authMocks = {
    status: (status: AuthStatusResponse = authAuthenticated) =>
        apiHttp.get("/api/auth/status", ({ response }) => response(200).json(status)),
    login: (result: AuthStatusResponse = authAuthenticated) =>
        apiHttp.post("/api/auth/login", ({ response }) => response(200).json(result)),
};

export const authAuthenticated: AuthStatusResponse = { authenticated: true };
export const authUnauthenticated: AuthStatusResponse = { authenticated: false };
