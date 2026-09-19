import { describe, expect, it } from "vitest";
import type { z } from "zod";
import {
    createAgentConfigurationSchema,
    editAgentConfigurationSchema,
    toAgentIdentifier,
    type ExistingAgent,
} from "@/pages/setup/add-capability-wizard/capability-wizard-validation";

type ConfigInput = z.input<ReturnType<typeof createAgentConfigurationSchema>>;

const EXISTING_AGENTS: ExistingAgent[] = [
    { agentId: "order-lookup-assistant", name: "Order Lookup Assistant" },
    { agentId: "user-directory-assistant", name: "User Directory Assistant" },
];

function agentConfiguration(overrides: Partial<ConfigInput> = {}): ConfigInput {
    return {
        name: "Support Assistant",
        identifier: "support-assistant",
        systemPrompt: "You are a support assistant.",
        sampleObject: '{"reply":""}',
        outputSchema: "",
        parameters: [],
        queries: [],
        actions: [],
        ...overrides,
    };
}

function parseIssues(config: ConfigInput, existingAgents: ExistingAgent[] = EXISTING_AGENTS) {
    const result = createAgentConfigurationSchema(existingAgents).safeParse(config);

    return result.success ? [] : result.error.issues.map((issue) => ({ ...issue, path: issue.path.join(".") }));
}

describe("toAgentIdentifier", () => {
    it("derives the identifier the way the server does", () => {
        expect(toAgentIdentifier(" Order-Lookup  Assistant! ")).toBe("order-lookup-assistant");
    });

    it("keeps the base letter of an accented character and marks the accent with a hyphen", () => {
        expect(toAgentIdentifier("Zamówienia")).toBe("zamo-wienia");
        expect(toAgentIdentifier("Łódź")).toBe("o-dz");
    });

    it("falls back to a usable identifier when the name has nothing to derive from", () => {
        expect(toAgentIdentifier("!!!")).toBe("agent");
        expect(toAgentIdentifier("顧客対応")).toBe("agent");
    });
});

describe("createAgentConfigurationSchema identifier", () => {
    it("requires an identifier", () => {
        expect(parseIssues(agentConfiguration({ identifier: " " }))).toMatchObject([
            { path: "identifier", message: "Identifier is required" },
        ]);
    });

    it("rejects an identifier the server would refuse", () => {
        for (const identifier of ["Support Assistant", "support--assistant", "support-assistant-", "zamówienia"]) {
            expect(parseIssues(agentConfiguration({ identifier }))).toMatchObject([
                { path: "identifier", message: "Use lowercase letters (a-z), digits (0-9) and single hyphens" },
            ]);
        }
    });
});

describe("createAgentConfigurationSchema existing agents", () => {
    it("accepts a name and identifier no other agent uses", () => {
        expect(parseIssues(agentConfiguration())).toEqual([]);
    });

    it("accepts any name when the app has no agents yet", () => {
        expect(parseIssues(agentConfiguration({ name: "Order Lookup Assistant" }), [])).toEqual([]);
    });

    it("rejects an identifier an existing agent already uses", () => {
        const issues = parseIssues(agentConfiguration({ identifier: "user-directory-assistant" }));

        expect(issues).toMatchObject([
            {
                path: "identifier",
                message: 'Another agent in this app already uses the identifier "user-directory-assistant"',
            },
        ]);
    });

    it("rejects a name an existing agent already uses, whatever its casing, and names that agent", () => {
        const issues = parseIssues(agentConfiguration({ name: "order lookup ASSISTANT" }));

        expect(issues).toMatchObject([
            { path: "name", message: 'This app already has an agent named "Order Lookup Assistant"' },
        ]);
    });

    it("reports a taken name and a taken identifier on their own fields", () => {
        const config = agentConfiguration({ name: "Order Lookup Assistant", identifier: "order-lookup-assistant" });

        expect(parseIssues(config).map((issue) => issue.path)).toEqual(["name", "identifier"]);
    });
});

describe("editAgentConfigurationSchema", () => {
    it("accepts a stored identifier the create schema would reject", () => {
        // The edit form neither shows nor sends the identifier, so an agent whose stored one
        // predates the create rules must still be saveable.
        const config = agentConfiguration({ identifier: "Legacy_Identifier" });

        expect(editAgentConfigurationSchema.safeParse(config).success).toBe(true);
        expect(parseIssues(config)).toMatchObject([{ path: "identifier" }]);
    });

    it("still validates the fields the edit form does show", () => {
        const result = editAgentConfigurationSchema.safeParse(agentConfiguration({ name: " " }));

        expect(result.success).toBe(false);
    });
});
