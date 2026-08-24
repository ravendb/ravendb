import { describe, expect, it } from "vitest";
import type { AiAgentParameterValueType } from "@/api/generated/server-api";
import { scalarError, toJsonValue, type ParameterFormValue } from "@/pages/apps/channels/agent-parameter-values";

function formValue(type: AiAgentParameterValueType, overrides: Partial<ParameterFormValue> = {}): ParameterFormValue {
    return { name: "p", type, text: "", flag: false, items: [], ...overrides };
}

describe("toJsonValue", () => {
    it("sends a number for a Number parameter, not the typed text", () => {
        expect(toJsonValue(formValue("Number", { text: " 25 " }))).toBe(25);
    });

    it("sends a boolean for a Boolean parameter", () => {
        expect(toJsonValue(formValue("Boolean", { flag: true }))).toBe(true);
        expect(toJsonValue(formValue("Boolean", { flag: false }))).toBe(false);
    });

    it("sends a trimmed string for String and Default parameters", () => {
        expect(toJsonValue(formValue("String", { text: " users/7 " }))).toBe("users/7");
        expect(toJsonValue(formValue("Default", { text: "users/7" }))).toBe("users/7");
    });

    it("sends arrays of the declared element type", () => {
        expect(toJsonValue(formValue("ArrayOfString", { items: [{ value: "emea" }, { value: " apac " }] }))).toEqual([
            "emea",
            "apac",
        ]);
        expect(toJsonValue(formValue("ArrayOfNumber", { items: [{ value: "1" }, { value: "2" }] }))).toEqual([1, 2]);
        expect(toJsonValue(formValue("ArrayOfBoolean", { items: [{ value: "true" }, { value: "False" }] }))).toEqual([
            true,
            false,
        ]);
    });

    it("sends null for a Null parameter", () => {
        expect(toJsonValue(formValue("Null"))).toBeNull();
    });
});

describe("scalarError", () => {
    it("requires a value", () => {
        expect(scalarError("String", "   ")).toBe("Required");
    });

    it("rejects text that is not the declared type", () => {
        expect(scalarError("Number", "abc")).toBe("Enter a number");
        expect(scalarError("Boolean", "maybe")).toBe("Enter true or false");
    });

    it("accepts text that is the declared type", () => {
        expect(scalarError("Number", "-3.5")).toBeNull();
        expect(scalarError("Boolean", "TRUE")).toBeNull();
        expect(scalarError("String", "users/1")).toBeNull();
    });
});
