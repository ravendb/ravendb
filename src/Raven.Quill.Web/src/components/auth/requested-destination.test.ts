import { describe, expect, it } from "vitest";
import { requestedDestination } from "@/components/auth/requested-destination";

describe("requestedDestination", () => {
    it("returns the path the guard recorded", () => {
        expect(requestedDestination({ from: "/usage" })).toBe("/usage");
        expect(requestedDestination({ from: "/apps/shop/agents?tab=all#top" })).toBe("/apps/shop/agents?tab=all#top");
    });

    it("returns null when nothing was recorded", () => {
        expect(requestedDestination(undefined)).toBeNull();
        expect(requestedDestination(null)).toBeNull();
        expect(requestedDestination({})).toBeNull();
        expect(requestedDestination({ from: 42 })).toBeNull();
    });

    it("drops a destination outside this app", () => {
        expect(requestedDestination({ from: "https://evil.example/steal" })).toBeNull();
        expect(requestedDestination({ from: "//evil.example/steal" })).toBeNull();
        expect(requestedDestination({ from: "/\\evil.example/steal" })).toBeNull();
        expect(requestedDestination({ from: "usage" })).toBeNull();
    });

    it("drops the login page so signing in cannot bounce back to it", () => {
        expect(requestedDestination({ from: "/login" })).toBeNull();
        expect(requestedDestination({ from: "/login?next=/usage" })).toBeNull();
    });
});
