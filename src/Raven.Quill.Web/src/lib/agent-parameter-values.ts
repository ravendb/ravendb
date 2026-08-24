import type { AiAgentParameterValueType } from "@/api/generated/server-api";

export function getAgentParameterPlaceholder(type: AiAgentParameterValueType): string {
    switch (type) {
        case "Number":
            return "e.g. 42";
        case "Boolean":
            return "true or false";
        case "ArrayOfString":
            return 'e.g. ["value1", "value2"]';
        case "ArrayOfNumber":
            return "e.g. [1, 2, 3]";
        case "ArrayOfBoolean":
            return "e.g. [true, false]";
        case "Null":
            return "null";
        case "String":
        case "Default":
            return "e.g. users/1";
    }
}

export function getAgentParameterValueError(value: string, type: AiAgentParameterValueType): string | null {
    switch (type) {
        case "Number":
            return isFiniteNumberToken(value) ? null : "Enter a valid Number value, e.g. 42";
        case "Boolean":
            return isBooleanToken(value) ? null : "Enter true or false";
        case "ArrayOfString":
            return isJsonArrayOf(value, (item) => typeof item === "string")
                ? null
                : 'Enter a String[] JSON array, e.g. ["value1", "value2"]';
        case "ArrayOfNumber":
            return isJsonArrayOf(value, (item) => typeof item === "number" && Number.isFinite(item))
                ? null
                : "Enter a Number[] JSON array, e.g. [1, 2, 3]";
        case "ArrayOfBoolean":
            return isJsonArrayOf(value, (item) => typeof item === "boolean")
                ? null
                : "Enter a Boolean[] JSON array, e.g. [true, false]";
        case "String":
        case "Default":
        case "Null":
            return null;
    }
}

function isFiniteNumberToken(value: string): boolean {
    const trimmed = value.trim();
    return trimmed !== "" && Number.isFinite(Number(trimmed));
}

function isBooleanToken(value: string): boolean {
    const normalized = value.trim().toLowerCase();
    return normalized === "true" || normalized === "false";
}

function isJsonArrayOf(value: string, isValidItem: (item: unknown) => boolean): boolean {
    try {
        const parsed: unknown = JSON.parse(value);
        return Array.isArray(parsed) && parsed.every(isValidItem);
    } catch {
        return false;
    }
}
