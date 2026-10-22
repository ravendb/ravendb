import { describe, expect, it } from "vitest";
import {
    describeConventions,
    describeWork,
    getActivityHeadline,
    getCollectionsToBuildFirst,
    getSuggestedReplies,
    getTurnOutcomes,
} from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import { createReplyTurn } from "@/pages/setup/add-app-wizard/steps/map/session/planner-frames";
import type { PlanCollection, PlanCollectionState } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import type { PlannerActivity, PlannerReplyTurn } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

function turn(activity: PlannerActivity[], status: PlannerReplyTurn["status"] = "done") {
    return { ...createReplyTurn(false), activity, status };
}

const built = (collection: string, version = 1): PlannerActivity => ({
    type: "registered",
    collection,
    version,
    warningCount: 0,
});

const failed = (collection: string): PlannerActivity => ({
    type: "rejected",
    collection,
    errorCount: 1,
    isChange: false,
});

const changeRejected = (collection: string): PlannerActivity => ({
    type: "rejected",
    collection,
    errorCount: 1,
    isChange: true,
});

describe("getTurnOutcomes", () => {
    it("lets a collection's last frame in the turn decide where it ended up", () => {
        expect(
            getTurnOutcomes([
                failed("Products"),
                built("Products"),
                built("Orders", 2),
                failed("Invoices"),
                changeRejected("Customers"),
            ]),
        ).toEqual({
            built: ["Products"],
            updated: ["Orders"],
            failed: ["Invoices"],
            kept: ["Customers"],
            removed: [],
        });
    });
});

describe("getActivityHeadline", () => {
    const progress = { builtCount: 2, totalCount: 3 };

    it("counts what the turn built against what it attempted, and calls out failures", () => {
        expect(getActivityHeadline(turn([built("Customers"), built("Orders"), failed("Products")]), progress)).toEqual({
            title: "Built 2 of 3 collections",
            detail: "1 failed",
            isDetailDestructive: true,
        });
    });

    it("counts updates as built when the turn also has failures to call out", () => {
        expect(
            getActivityHeadline(turn([built("Customers", 2), built("Orders"), failed("Products")]), progress).title,
        ).toBe("Built 2 of 3 collections");
    });

    it("reports a turn that only built, with where the whole plan stands", () => {
        expect(getActivityHeadline(turn([built("Customers"), built("Orders")]), progress)).toEqual({
            title: "Built 2 collections",
            detail: "2 of 3 built",
            isDetailDestructive: false,
        });
    });

    it("reports updates together with what the turn built, and where the whole plan stands", () => {
        expect(
            getActivityHeadline(turn([built("Products"), built("Orders", 2)]), { builtCount: 3, totalCount: 3 }),
        ).toEqual({ title: "Updated 2 collections", detail: "3 of 3 built", isDetailDestructive: false });
    });

    it("does not count a rejected change to a built collection as a failed collection", () => {
        expect(getActivityHeadline(turn([changeRejected("Orders")]), { builtCount: 1, totalCount: 1 })).toEqual({
            title: "Kept 1 collection as built",
            detail: "1 change rejected",
            isDetailDestructive: true,
        });
    });

    it("says the turn is working until it builds something", () => {
        expect(getActivityHeadline(turn([{ type: "note", text: "Checked keys." }], "working"), progress)).toEqual({
            title: "Working on your prompt",
            detail: "",
            isDetailDestructive: false,
        });
        expect(getActivityHeadline(turn([built("Orders")], "working"), progress)).toEqual({
            title: "Building collections",
            detail: "2 of 3 built",
            isDetailDestructive: false,
        });
    });

    it("falls back to the plan for a turn that touched no collection", () => {
        expect(getActivityHeadline(turn([{ type: "note", text: "Checked keys." }]), progress).title).toBe(
            "Updated the plan",
        );
    });
});

describe("describeWork", () => {
    it("only claims building once the turn registered or rejected a collection", () => {
        expect(describeWork(turn([], "working"))).toBe("Working on your prompt");
        expect(describeWork(turn([failed("Products")], "working"))).toBe("Building collections");
    });

    it("claims building once an attempt failed and is being retried", () => {
        const retrying: PlannerActivity = {
            type: "transientError",
            collection: "Products",
            errors: ["x"],
            rejections: 1,
            maxRejections: 2,
        };

        expect(describeWork(turn([retrying], "working"))).toBe("Building collections");
    });
});

describe("getCollectionsToBuildFirst", () => {
    const planCollection = (name: string, state: PlanCollectionState): PlanCollection => ({
        name,
        state,
        area: null,
        why: null,
        rootTable: null,
        document: null,
        previousDocument: null,
        config: null,
        warnings: [],
        errors: [],
        searchKey: name.toLowerCase(),
    });

    it("names every suggested collection", () => {
        expect(
            getCollectionsToBuildFirst([
                planCollection("Orders", "suggested"),
                planCollection("Customers", "suggested"),
                planCollection("Invoices", "failed"),
            ]),
        ).toEqual(["Orders", "Customers"]);
    });

    it("offers nothing once a collection is built, or when nothing is suggested", () => {
        expect(
            getCollectionsToBuildFirst([planCollection("Orders", "suggested"), planCollection("Customers", "built")]),
        ).toEqual([]);
        expect(getCollectionsToBuildFirst([planCollection("Invoices", "failed")])).toEqual([]);
    });
});

describe("getSuggestedReplies", () => {
    it("offers to retry a failed collection, or to carry on without it", () => {
        expect(getSuggestedReplies({ failed: ["Products"], kept: [] })).toEqual([
            { label: "Retry Products", prompt: "Retry Products.", isRetry: true, keeps: [] },
            { label: "Carry on without Products", prompt: "Carry on without Products.", isRetry: false, keeps: [] },
        ]);
    });

    it("folds several failures into one reply each", () => {
        expect(
            getSuggestedReplies({ failed: ["Invoices", "Shipments", "WorkOrders"], kept: [] }).map(
                (reply) => reply.label,
            ),
        ).toEqual(["Retry the 3 failed", "Carry on without them"]);
    });

    it("offers to keep a built collection whose change was rejected, never to drop it, and lets go of the rejection", () => {
        expect(getSuggestedReplies({ failed: [], kept: ["Orders"] })).toEqual([
            { label: "Retry the change to Orders", prompt: "Retry the change to Orders.", isRetry: true, keeps: [] },
            { label: "Keep the previous version", prompt: "Keep Orders as it is.", isRetry: false, keeps: ["Orders"] },
        ]);
    });

    it("offers nothing when nothing failed", () => {
        expect(getSuggestedReplies({ failed: [], kept: [] })).toEqual([]);
    });
});

describe("describeConventions", () => {
    const conventions: Omit<Extract<PlannerActivity, { type: "conventions" }>, "propertyCase"> = {
        type: "conventions",
        propertyLanguage: null,
        notes: null,
        mustReEmit: [],
    };

    it("writes the casing the way it looks", () => {
        expect(describeConventions({ ...conventions, propertyCase: "CamelCase" })).toBe(
            "Property names set to camelCase",
        );
        expect(describeConventions({ ...conventions, propertyCase: "SnakeCase" })).toBe(
            "Property names set to snake_case",
        );
    });

    it("says which collections take the new naming, and in what language", () => {
        expect(
            describeConventions({
                ...conventions,
                propertyCase: "Unspecified",
                propertyLanguage: "Spanish",
                mustReEmit: ["Orders", "Products"],
            }),
        ).toBe("Property names in Spanish, re-emitting Orders and Products");
    });
});
