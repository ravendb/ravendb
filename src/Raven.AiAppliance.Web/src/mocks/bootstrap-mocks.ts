import type { BootstrapStatusResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const bootstrapMocks = {
    status: (status: BootstrapStatusResponse = bootstrapReady) =>
        apiHttp.get("/api/bootstrap/status", ({ response }) => response(200).json(status)),
    redeemLicense: (result: BootstrapStatusResponse = { state: "Redeeming" }) =>
        apiHttp.post("/api/bootstrap/redeem-license", ({ response }) => response(200).json(result)),
};

export const bootstrapReady: BootstrapStatusResponse = { state: "Ready" };
