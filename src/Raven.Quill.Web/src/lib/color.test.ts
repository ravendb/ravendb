import { describe, expect, it } from "vitest";
import { formatAs, hslToRgb, parseColor, rgbToHsl, toHex } from "@/lib/color";

describe("parseColor", () => {
    it("reads a six digit hex", () => {
        expect(parseColor("#2f6f4f")).toEqual({ r: 47, g: 111, b: 79 });
    });

    it("expands a three digit hex", () => {
        expect(parseColor("#abc")).toEqual({ r: 170, g: 187, b: 204 });
    });

    it("accepts hex regardless of the assumed format", () => {
        expect(parseColor("#2f6f4f", "hsl")).toEqual({ r: 47, g: 111, b: 79 });
    });

    it("reads a functional rgb value", () => {
        expect(parseColor("rgb(47, 111, 79)")).toEqual({ r: 47, g: 111, b: 79 });
    });

    it("reads a bare triple as rgb by default", () => {
        expect(parseColor("47, 111, 79")).toEqual({ r: 47, g: 111, b: 79 });
    });

    it("reads a percent bearing triple as hsl even when rgb is assumed", () => {
        expect(parseColor("150, 41%, 31%", "rgb")).toEqual({ r: 47, g: 111, b: 79 });
    });

    it("reads a bare triple as hsl when hsl is assumed", () => {
        expect(parseColor("150, 41, 31", "hsl")).toEqual({ r: 47, g: 111, b: 79 });
    });

    it("clamps out of range channels", () => {
        expect(parseColor("300, -20, 400")).toEqual({ r: 255, g: 0, b: 255 });
    });

    it.each(["", "   ", "#gg0011", "#12", "1, 2", "1, 2, 3, 4", "teal"])("returns null for %o", (input) => {
        expect(parseColor(input)).toBeNull();
    });
});

describe("formatAs", () => {
    it("renders hex in lower case", () => {
        expect(formatAs("hex", "#2F6F4F")).toBe("#2f6f4f");
    });

    it("renders rgb as a bare triple", () => {
        expect(formatAs("rgb", "#2f6f4f")).toBe("47, 111, 79");
    });

    it("renders hsl with percent units", () => {
        expect(formatAs("hsl", "#2f6f4f")).toBe("150, 41%, 31%");
    });
});

describe("round tripping", () => {
    it.each(["#2f6f4f", "#0d1117", "#ffffff", "#000000"])("is stable through hsl for %s", (hex) => {
        expect(toHex(hslToRgb(rgbToHsl(parseColor(hex)!)))).toBe(hex);
    });

    // Integer HSL cannot address every 24 bit colour, so a trip through it can land up to a few
    // units away per channel. Pinned rather than papered over: the popover shows hex alongside the
    // other formats precisely so an operator who cares about the exact value can see it.
    it.each([
        ["#ff775f", "#ff7961"],
        ["#e0ece6", "#dfece6"],
    ])("drifts %s to %s through hsl", (hex, drifted) => {
        expect(toHex(hslToRgb(rgbToHsl(parseColor(hex)!)))).toBe(drifted);
    });

    it("is exact through rgb", () => {
        expect(toHex(parseColor(formatAs("rgb", "#ff775f"))!)).toBe("#ff775f");
    });
});
