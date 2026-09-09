import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

type MapSourceOption = {
    value: AppFormData["map"]["source"];
    label: string;
    description: string;
};

export const AI_SUGGEST_OPTION: MapSourceOption = {
    value: "ai-suggested",
    label: "AI Suggest",
    description:
        "Reads your schema and works out the whole mapping on its own. Add an intent prompt only if " +
        "you want to steer particular choices.",
};

export const MANUAL_OPTION: MapSourceOption = {
    value: "manual",
    label: "Manual",
    description: "Starts from an empty form scaffolded from your schema. You decide what to flatten, embed, or link.",
};

export const MAP_SOURCE_OPTIONS: MapSourceOption[] = [AI_SUGGEST_OPTION, MANUAL_OPTION];
