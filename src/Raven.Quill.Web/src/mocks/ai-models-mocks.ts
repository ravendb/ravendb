import type { AiModelsResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const aiModelsMocks = {
    list: (response: AiModelsResponse = sampleModels) =>
        apiHttp.post("/api/ai/models", ({ response: respond }) => respond(200).json(response)),
};

export const sampleModels: AiModelsResponse = {
    models: ["gpt-4o", "gpt-4o-mini", "gpt-4.1", "gpt-4.1-mini", "o3", "o4-mini"],
};
