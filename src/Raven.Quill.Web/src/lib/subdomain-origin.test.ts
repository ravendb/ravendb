import { afterEach, describe, expect, it, vi } from "vitest";
import { containerNameForHost, originForSubdomain } from "@/lib/subdomain-origin";

function setLocation(href: string) {
    const { protocol, hostname, port, origin } = new URL(href);
    vi.stubGlobal("window", { location: { protocol, hostname, port, origin } });
}

afterEach(() => {
    vi.unstubAllGlobals();
});

describe("originForSubdomain", () => {
    it("swaps the leading label on appliance hosts", () => {
        setLocation("https://dashboard.user-test.myquill.ai");
        expect(originForSubdomain("api")).toBe("https://api.user-test.myquill.ai");
    });

    it("swaps the leading label whatever the current role is", () => {
        setLocation("https://api.acme.example.com");
        expect(originForSubdomain("public")).toBe("https://public.acme.example.com");
    });

    it("keeps a non-default port", () => {
        setLocation("https://dashboard.acme.example.com:8443");
        expect(originForSubdomain("public")).toBe("https://public.acme.example.com:8443");
    });

    it("falls back to the current origin on apex hosts instead of handing out a foreign domain", () => {
        setLocation("https://example.com");
        expect(originForSubdomain("api")).toBe("https://example.com");

        setLocation("https://example.co.uk");
        expect(originForSubdomain("api")).toBe("https://example.co.uk");
    });

    it("falls back to the current origin on hosts with no slug label", () => {
        setLocation("https://dashboard.example.com");
        expect(originForSubdomain("api")).toBe("https://dashboard.example.com");
    });

    it("falls back to the current origin on single-label and LAN hosts", () => {
        setLocation("http://localhost:5173");
        expect(originForSubdomain("api")).toBe("http://localhost:5173");

        setLocation("https://quill.local");
        expect(originForSubdomain("api")).toBe("https://quill.local");
    });

    it("falls back to the current origin on bare IPs, which have four labels of their own", () => {
        setLocation("https://10.0.0.5");
        expect(originForSubdomain("api")).toBe("https://10.0.0.5");
    });
});

describe("containerNameForHost", () => {
    it("reads the slug from appliance hosts", () => {
        expect(containerNameForHost("dashboard.user-test.myquill.ai")).toBe("user-test");
        expect(containerNameForHost("dashboard.acme.example.com")).toBe("acme");
    });

    it("falls back to the compose name for shorter hosts and bare IPs", () => {
        expect(containerNameForHost("dashboard.example.com")).toBe("quill");
        expect(containerNameForHost("example.com")).toBe("quill");
        expect(containerNameForHost("localhost")).toBe("quill");
        expect(containerNameForHost("10.0.0.5")).toBe("quill");
    });
});
