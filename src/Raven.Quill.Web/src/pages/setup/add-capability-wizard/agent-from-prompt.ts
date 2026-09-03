import { api } from "@/api/api";
import { AI_CONSENT_REQUIRED_MESSAGE } from "@/api/custom-services/assistant-service";
import type { AiAgentConfiguration } from "@/api/generated/server-api";

// Asks the AI service for a single agent candidate derived from the operator's free-text
// intent ("from-prompt" mode). Throws on a non-success status or an empty result so callers
// (the create-step beforeNext and the review-step regenerate) can surface the rationale.
export async function generateAgentFromPrompt(slug: string, intentPrompt: string): Promise<AiAgentConfiguration> {
    const result = await api.services.apps.suggestAgent(slug, {
        mode: "from-prompt",
        intentPrompt,
    });

    if (result.status === "ConsentRequired") {
        throw new Error(AI_CONSENT_REQUIRED_MESSAGE);
    }

    if (result.status !== "Success" || result.configurations.length === 0) {
        throw new Error(result.rationale?.filter(Boolean).join("\n") || `AI suggestion failed (${result.status}).`);
    }

    return result.configurations[0];
}
