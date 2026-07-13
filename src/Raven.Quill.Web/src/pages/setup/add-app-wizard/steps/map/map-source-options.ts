import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

type MapSourceOption = {
    value: AppFormData["map"]["source"];
    label: string;
    description: string;
};

export const AI_SUGGEST_OPTION: MapSourceOption = {
    value: "ai-suggested",
    label: "AI Suggest",
    description: "LLM proposes a draft CDCSinkConfiguration based on schema + your intent prompt.",
};

export const MANUAL_OPTION: MapSourceOption = {
    value: "manual",
    label: "Manual",
    description: "Empty form scaffolded from the discovered schema. You pick what to flat / embed / link.",
};

export const MAP_SOURCE_OPTIONS: MapSourceOption[] = [AI_SUGGEST_OPTION, MANUAL_OPTION];
