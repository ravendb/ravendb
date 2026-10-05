import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableKey } from "@/pages/setup/add-app-wizard/discover-utils";

export function computeMapKey(map: {
    sourceKey: string;
    source: AppFormData["map"]["source"];
    aiPrompt: string;
    selectedTables: AppFormData["verifySchema"]["tables"];
}): string {
    return JSON.stringify({
        sourceKey: map.sourceKey,
        source: map.source,
        aiPrompt: map.source === "ai-suggested" ? map.aiPrompt.trim() : "",
        selectedTables: map.selectedTables.map(getTableKey).sort(),
    });
}
