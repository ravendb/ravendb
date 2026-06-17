import type { BootstrapStatusResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const bootstrapMocks = {
    status: (status: BootstrapStatusResponse = bootstrapReady) =>
        apiHttp.get("/api/bootstrap/status", ({ response }) => response(200).json(status)),
};

export const bootstrapReady: BootstrapStatusResponse = { state: "Ready" };
