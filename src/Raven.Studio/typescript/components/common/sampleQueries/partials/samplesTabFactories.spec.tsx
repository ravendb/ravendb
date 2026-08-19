import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { createMethodsTab, createSampleScriptsTab } from "./samplesTabFactories";

describe("samplesTabFactories", () => {
    it("createSampleScriptsTab returns a scripts tab without search", () => {
        const tab = createSampleScriptsTab([]);

        expect(tab.key).toBe("scripts");
        expect(tab.label).toBe("Sample scripts");
        expect(tab.icon).toBe("document");
        expect(tab.hasSearch).toBeUndefined();
        expect(typeof tab.content).toBe("function");
    });

    it("createSampleScriptsTab content renders the provided scripts", () => {
        const tab = createSampleScriptsTab([{ title: "T1", description: "", script: "x" }]);

        const { screen } = rtlRender(<>{tab.content({ onSelect: jest.fn(), search: "" })}</>);

        expect(screen.getByText("T1")).toBeInTheDocument();
    });

    it("createMethodsTab returns a methods tab with search", () => {
        const tab = createMethodsTab([]);

        expect(tab.key).toBe("methods");
        expect(tab.label).toBe("Methods");
        expect(tab.icon).toBe("indent");
        expect(tab.hasSearch).toBe(true);
        expect(tab.searchPlaceholder).toBe("Search by signature");
        expect(typeof tab.content).toBe("function");
    });

    it("createMethodsTab content shows methods matching the search", () => {
        const tab = createMethodsTab([{ category: "Cat", methods: [{ signature: "sig()", description: "d" }] }]);

        const { screen } = rtlRender(<>{tab.content({ onSelect: jest.fn(), search: "" })}</>);

        expect(screen.getByText("sig()")).toBeInTheDocument();
    });

    it("createMethodsTab content hides methods not matching the search", () => {
        const tab = createMethodsTab([{ category: "Cat", methods: [{ signature: "sig()", description: "d" }] }]);

        const { screen } = rtlRender(<>{tab.content({ onSelect: jest.fn(), search: "zzz" })}</>);

        expect(screen.queryByText("sig()")).not.toBeInTheDocument();
    });
});
