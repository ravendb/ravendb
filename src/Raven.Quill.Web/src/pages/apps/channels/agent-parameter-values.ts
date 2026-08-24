import type { AgentParameterSummary, AiAgentParameterValueType } from "@/api/generated/server-api";

export type ParameterFormValue = {
    name: string;
    type: AiAgentParameterValueType;
    text: string;
    flag: boolean;
    items: { value: string }[];
};

export const PARAMETER_VALUE_TYPES = [
    "Default",
    "String",
    "Number",
    "Boolean",
    "ArrayOfString",
    "ArrayOfNumber",
    "ArrayOfBoolean",
    "Null",
] as const satisfies readonly AiAgentParameterValueType[];

const ARRAY_TYPES: AiAgentParameterValueType[] = ["ArrayOfString", "ArrayOfNumber", "ArrayOfBoolean"];

export function isArrayType(type: AiAgentParameterValueType): boolean {
    return ARRAY_TYPES.includes(type);
}

export function elementTypeOf(type: AiAgentParameterValueType): AiAgentParameterValueType {
    switch (type) {
        case "ArrayOfNumber":
            return "Number";
        case "ArrayOfBoolean":
            return "Boolean";
        default:
            return "String";
    }
}

export function defaultFormValue(parameter: AgentParameterSummary): ParameterFormValue {
    return {
        name: parameter.name,
        type: parameter.type,
        text: "",
        flag: false,
        items: isArrayType(parameter.type) ? [{ value: "" }] : [],
    };
}

export function parseNumber(text: string): number | null {
    const trimmed = text.trim();
    if (trimmed === "") {
        return null;
    }
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
}

export function parseBoolean(text: string): boolean | null {
    const normalized = text.trim().toLowerCase();
    if (normalized === "true") {
        return true;
    }
    if (normalized === "false") {
        return false;
    }
    return null;
}

export function scalarError(type: AiAgentParameterValueType, text: string): string | null {
    if (text.trim() === "") {
        return "Required";
    }
    if (type === "Number" && parseNumber(text) === null) {
        return "Enter a number";
    }
    if (type === "Boolean" && parseBoolean(text) === null) {
        return "Enter true or false";
    }
    return null;
}

export function toJsonValue(value: ParameterFormValue): unknown {
    switch (value.type) {
        case "Null":
            return null;
        case "Number":
            return parseNumber(value.text);
        case "Boolean":
            return value.flag;
        case "ArrayOfString":
            return value.items.map((item) => item.value.trim());
        case "ArrayOfNumber":
            return value.items.map((item) => parseNumber(item.value));
        case "ArrayOfBoolean":
            return value.items.map((item) => parseBoolean(item.value));
        default:
            return value.text.trim();
    }
}

export function placeholderFor(type: AiAgentParameterValueType): string {
    switch (type) {
        case "Number":
        case "ArrayOfNumber":
            return "e.g. 42";
        case "Boolean":
        case "ArrayOfBoolean":
            return "true or false";
        default:
            return "e.g. users/1";
    }
}

export function snippetValueFor(type: AiAgentParameterValueType): unknown {
    switch (type) {
        case "Number":
            return 0;
        case "Boolean":
            return true;
        case "ArrayOfString":
            return ["<value>"];
        case "ArrayOfNumber":
            return [0];
        case "ArrayOfBoolean":
            return [true];
        case "Null":
            return null;
        default:
            return "<value>";
    }
}

export type SnippetSyntax = "json" | "csharp" | "python";

export function snippetLiteralFor(syntax: SnippetSyntax, value: unknown): string {
    if (Array.isArray(value)) {
        const items = value.map((item) => snippetLiteralFor(syntax, item)).join(", ");
        return syntax === "csharp" ? `new[] { ${items} }` : `[${items}]`;
    }

    if (syntax === "python") {
        if (value === null) {
            return "None";
        }
        if (typeof value === "boolean") {
            return value ? "True" : "False";
        }
    }

    return JSON.stringify(value);
}

export function typeLabelFor(type: AiAgentParameterValueType): string | null {
    switch (type) {
        case "Default":
            return null;
        case "ArrayOfString":
            return "array of strings";
        case "ArrayOfNumber":
            return "array of numbers";
        case "ArrayOfBoolean":
            return "array of booleans";
        default:
            return type.toLowerCase();
    }
}
