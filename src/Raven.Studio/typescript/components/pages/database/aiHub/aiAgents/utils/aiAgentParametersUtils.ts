import * as yup from "yup";
import assertUnreachable from "components/utils/assertUnreachable";

type AiAgentParameterValueType = Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType;

export interface AiAgentParameterTypeInfo {
    label: string;
    exampleValue?: string;
}

interface AiAgentParameterValueValidationResult {
    isValid: boolean;
    message?: string;
}

function createValueSchema<T extends yup.StringSchema<string | null | undefined>>(schema: T): T {
    return schema.test("ai-agent-parameter-value", function (value, { parent }) {
        const result = validateParameterValue(value, parent?.type);

        if (result.isValid) {
            return true;
        }

        return this.createError({ message: result.message });
    }) as T;
}

function getParameterTypeInfo(type: AiAgentParameterValueType): AiAgentParameterTypeInfo {
    switch (type) {
        case "ArrayOfString":
            return { label: "String[]", exampleValue: '["value1", "value2", "value3"]' };
        case "ArrayOfNumber":
            return { label: "Number[]", exampleValue: "[1, 2, 3]" };
        case "ArrayOfBoolean":
            return { label: "Boolean[]", exampleValue: "[true, false, true]" };
        case "Default":
            return { label: "Any" };
        case "String":
        case "Number":
        case "Boolean":
        case "Null":
            return { label: type };
        default:
            assertUnreachable(type);
    }
}

function validateParameterValue(
    value: unknown,
    type: AiAgentParameterValueType
): AiAgentParameterValueValidationResult {
    if (value == null || value === "" || type == null) {
        return { isValid: true };
    }

    switch (type) {
        case "Number":
            return isFiniteNumberToken(value)
                ? { isValid: true }
                : { isValid: false, message: "Value must be a valid Number value" };
        case "Boolean":
            return isBooleanToken(value)
                ? { isValid: true }
                : { isValid: false, message: "Value must be a valid Boolean value" };
        case "ArrayOfString":
            return validateArrayValue(value, (item) => typeof item === "string", type);
        case "ArrayOfNumber":
            return validateArrayValue(value, isFiniteNumberToken, type);
        case "ArrayOfBoolean":
            return validateArrayValue(value, isBooleanToken, type);
        case "String":
        case "Null":
        case "Default":
            return { isValid: true };
        default:
            assertUnreachable(type);
    }
}

function mapParameterValueToType(value: string, type: AiAgentParameterValueType) {
    switch (type) {
        case "Number":
            return Number(value);
        case "Boolean":
            return mapBooleanToken(value);
        case "ArrayOfString":
            return requireArrayItems(value);
        case "ArrayOfNumber":
            return requireArrayItems(value).map((item) => Number(item));
        case "ArrayOfBoolean":
            return requireArrayItems(value).map(mapBooleanToken);
        case "Null":
            return null;
        case "String":
        case "Default":
        default:
            return value;
    }
}

function validateArrayValue(
    value: unknown,
    isValidItem: (item: unknown) => boolean,
    type: AiAgentParameterValueType
): AiAgentParameterValueValidationResult {
    const arrayItems = getArrayItems(value);
    if (!arrayItems) {
        return { isValid: false, message: getArrayValidationMessage(type) };
    }

    return arrayItems.items.every(isValidItem)
        ? { isValid: true }
        : { isValid: false, message: getArrayValidationMessage(type) };
}

function getArrayValidationMessage(type: AiAgentParameterValueType): string {
    const { label, exampleValue } = getParameterTypeInfo(type);
    return `Value must be a valid ${label} value, e.g. ${exampleValue}`;
}

function getArrayItems(value: unknown): { items: unknown[] } | null {
    if (Array.isArray(value)) {
        return { items: value };
    }

    if (typeof value !== "string") {
        return null;
    }

    const parsedJson = tryParseJson(value.trim());

    if (Array.isArray(parsedJson)) {
        return { items: parsedJson };
    }

    return null;
}

function requireArrayItems(value: unknown): unknown[] {
    const arrayItems = getArrayItems(value);
    if (!arrayItems) {
        throw new Error("Expected AI agent parameter array value in JSON array format");
    }

    return arrayItems.items;
}

function tryParseJson(value: string): unknown {
    try {
        return JSON.parse(value);
    } catch {
        return null;
    }
}

function isFiniteNumberToken(value: unknown): boolean {
    if (typeof value === "number") {
        return Number.isFinite(value);
    }

    if (typeof value !== "string") {
        return false;
    }

    const trimmedValue = value.trim();
    return trimmedValue !== "" && Number.isFinite(Number(trimmedValue));
}

function isBooleanToken(value: unknown): boolean {
    if (typeof value === "boolean") {
        return true;
    }

    if (typeof value !== "string") {
        return false;
    }

    const normalizedValue = value.trim().toLowerCase();
    return normalizedValue === "true" || normalizedValue === "false";
}

function mapBooleanToken(value: unknown): boolean {
    return typeof value === "boolean" ? value : value.toString().trim().toLowerCase() === "true";
}

function formatParameterValueForDisplay(value: unknown): string {
    if (value === null) {
        return "null";
    }

    if (typeof value === "string") {
        return value;
    }

    if (typeof value === "number" || typeof value === "boolean") {
        return value.toString();
    }

    return JSON.stringify(value);
}

export const aiAgentParametersUtils = {
    createValueSchema,
    formatParameterValueForDisplay,
    getParameterTypeInfo,
    mapParameterValueToType,
    validateParameterValue,
};
