
import { autocomplete } from "../autocompleteUtils";
import { AUTOCOMPLETE_META } from "../../src/providers/common";

const orderingTypes: string[] = ["string", "long", "double", "alphanumeric"];


describe("can complete order by ", function () {
    it("can complete fields in order by - at root level", async () => {
        const suggestions = await autocomplete("from Orders order by |");

        const companyField = suggestions.find(x => x.value.startsWith("Company") && x.meta === AUTOCOMPLETE_META.field);
        expect(companyField)
            .toBeTruthy();
    });

    it("can complete fields in order by - nested field", async () => {
        const suggestions = await autocomplete("from Orders order by Lines[].|");

        const companyField = suggestions.find(x => x.value.startsWith("Discount") && x.meta === AUTOCOMPLETE_META.field);
        expect(companyField)
            .toBeTruthy();
    });

    it("can complete order by random()", async () => {
        const suggestions = await autocomplete("from Orders order by |");

        const randomField = suggestions.find(x => x.value.startsWith("random()"));
        expect(randomField)
            .toBeTruthy();
    });

    it("can complete order by score()", async () => {
        const suggestions = await autocomplete("from Orders order by |");

        const scoreField = suggestions.find(x => x.value.startsWith("score()"));
        expect(scoreField)
            .toBeTruthy();
    });
    
    it("can complete next keywords after field", async () => {
        const suggestions = await autocomplete("from Orders order by Company |");

        const nextKeywords = ["select", "include", "limit"];

        for (const keyword of nextKeywords) {
            expect(suggestions.find(x => x.value.startsWith(keyword)))
                .toBeTruthy();
        }
    });
    
    it("can complete as operator after field", async () => {
        const suggestions = await autocomplete("from Orders order by Company |");

        expect(suggestions.find(x => x.value.startsWith("as")))
            .toBeTruthy();
    });
    
    it("can complete ordering type", async () => {
        const suggestions = await autocomplete("from Orders order by Company as |");

        for (const orderType of orderingTypes) {
            expect(suggestions.find(x => x.value.startsWith(orderType)))
                .toBeTruthy();
        }
    });
    
    it("can complete asc/desc when NO ordering type specified", async () => {
        const suggestions = await autocomplete("from Orders order by Company |");
        
        expect(suggestions.find(x => x.value.startsWith("asc ")))
            .toBeTruthy();
        expect(suggestions.find(x => x.value.startsWith("desc ")))
            .toBeTruthy();
    });

    it("can complete asc/desc when ordering type specified", async () => {
        const suggestions = await autocomplete("from Orders order by Company as string |");

        expect(suggestions.find(x => x.value.startsWith("asc ")))
            .toBeTruthy();
        expect(suggestions.find(x => x.value.startsWith("desc ")))
            .toBeTruthy();
    });

    it("can complete nulls first/last after field", async () => {
        const suggestions = await autocomplete("from Orders order by Company |");

        const nullsFirst = suggestions.find(x => x.value.startsWith("nulls first"));
        const nullsLast = suggestions.find(x => x.value.startsWith("nulls last"));

        expect(nullsFirst).toBeTruthy();
        expect(nullsFirst.caption).toEqual("nulls first");
        expect(nullsFirst.meta).toEqual(AUTOCOMPLETE_META.keyword);

        expect(nullsLast).toBeTruthy();
        expect(nullsLast.caption).toEqual("nulls last");
        expect(nullsLast.meta).toEqual(AUTOCOMPLETE_META.keyword);
    });

    it("does NOT suggest standalone 'nulls' token", async () => {
        const suggestions = await autocomplete("from Orders order by Company |");

        const standaloneNulls = suggestions.find(x => x.caption === "nulls");
        expect(standaloneNulls).toBeFalsy();
    });

    it("can complete nulls first/last after asc", async () => {
        const suggestions = await autocomplete("from Orders order by Company asc |");

        expect(suggestions.find(x => x.value.startsWith("nulls first")))
            .toBeTruthy();
        expect(suggestions.find(x => x.value.startsWith("nulls last")))
            .toBeTruthy();
    });

    it("can complete nulls first/last after desc", async () => {
        const suggestions = await autocomplete("from Orders order by Company desc |");

        expect(suggestions.find(x => x.value.startsWith("nulls first")))
            .toBeTruthy();
        expect(suggestions.find(x => x.value.startsWith("nulls last")))
            .toBeTruthy();
    });

    it("can complete nulls first/last after as <type>", async () => {
        const suggestions = await autocomplete("from Orders order by Company as string |");

        expect(suggestions.find(x => x.value.startsWith("nulls first")))
            .toBeTruthy();
        expect(suggestions.find(x => x.value.startsWith("nulls last")))
            .toBeTruthy();
    });

    it("can complete nulls first/last after as <type> <direction>", async () => {
        const suggestions = await autocomplete("from Orders order by Company as long desc |");

        expect(suggestions.find(x => x.value.startsWith("nulls first")))
            .toBeTruthy();
        expect(suggestions.find(x => x.value.startsWith("nulls last")))
            .toBeTruthy();
    });

    it("does NOT suggest nulls first/last after nulls clause already used", async () => {
        const suggestions = await autocomplete("from Orders order by Company nulls first |");

        expect(suggestions.find(x => x.value.startsWith("nulls first")))
            .toBeFalsy();
        expect(suggestions.find(x => x.value.startsWith("nulls last")))
            .toBeFalsy();
    });

    it("can complete next keywords after nulls first", async () => {
        const suggestions = await autocomplete("from Orders order by Company nulls first |");

        const nextKeywords = ["select", "include", "limit"];

        for (const keyword of nextKeywords) {
            expect(suggestions.find(x => x.value.startsWith(keyword)))
                .toBeTruthy();
        }
    });

    it("can complete fields in second order by item after nulls first", async () => {
        const suggestions = await autocomplete("from Orders order by Company nulls first, |");

        const companyField = suggestions.find(x => x.value.startsWith("Freight") && x.meta === AUTOCOMPLETE_META.field);
        expect(companyField)
            .toBeTruthy();
    });
})
