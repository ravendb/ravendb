import { autocomplete } from "../autocompleteUtils";

const groupByFunctions = ["key()", "count()", "sum("];


describe("can complete select", function () {
    it("can complete fields - first field", async () => {
        const suggestions = await autocomplete("from Orders select | ");
        
        const expectedFields = ["Company", "Employee"];
        
        for (const field of expectedFields) {
            expect(suggestions.find(x => x.value.startsWith(field)))
                .toBeTruthy();
        }
    });

    it("can complete fields - nested field - just after dot", async () => {
        const suggestions = await autocomplete("from Orders select Lines[].| ");

        const expectedFields = ["Discount", "Product"];

        for (const field of expectedFields) {
            expect(suggestions.find(x => x.value.startsWith(field)))
                .toBeTruthy();
        }
    });

    it("can complete fields - nested field - partial", async () => {
        const suggestions = await autocomplete("from Orders select Lines[].Di| ");

        const expectedFields = ["Discount"];

        for (const field of expectedFields) {
            expect(suggestions.find(x => x.value.startsWith(field)))
                .toBeTruthy();
        }
    });

    it("can complete fields - next field", async () => {
        const suggestions = await autocomplete("from Orders select Field1,  | ");

        const expectedFields = ["Company", "Employee"];

        for (const field of expectedFields) {
            expect(suggestions.find(x => x.value.startsWith(field)))
                .toBeTruthy();
        }
    });
    
    it("doesn't complete keywords in select", async () => {
        const nextKeywords = ["limit", "include"];

        const suggestions = await autocomplete("from Orders select  | ");

        for (const keyword of nextKeywords) {
            expect(suggestions.find(x => x.value.startsWith(keyword)))
                .toBeFalsy();
        }
    });
    
    it("has empty list when defining select as alias", async () => {
        const suggestions = await autocomplete("from Orders select Company as |");
        
        expect(suggestions)
            .toBeEmpty();
    });
    
    it("can suggest distinct in select stmt", async () => {
        const suggestions = await autocomplete("from Orders select | ");
        
        expect(suggestions.find(x => x.value.startsWith("distinct")))
            .toBeTruthy();
    });

    it("doesn't suggest distinct in select stmt, when after field", async () => {
        const suggestions = await autocomplete("from Orders select Name, | ");

        expect(suggestions.find(x => x.value.startsWith("distinct")))
            .toBeFalsy();
    });
    
    it("doesn't suggest group functions when no group by", async () => {
        const suggestions = await autocomplete("from Orders select |");

        for (const func of groupByFunctions) {
            expect(suggestions.find(x => x.value.startsWith(func)))
                .toBeFalsy();
        }
    });

    it("can suggest group functions when group by is defined", async () => {
        const suggestions = await autocomplete("from Orders group by Company select |");

        for (const func of groupByFunctions) {
            expect(suggestions.find(x => x.value.startsWith(func)))
                .toBeTruthy();
        }
    });
    
    it("can complete suggest() function in select", async () => {
        const suggestions = await autocomplete("from Orders select |");
        
        expect(suggestions.find(x => x.value.startsWith("suggest(")))
            .toBeTruthy();
    });
    
    it("can complete declared functions in select", async () => {
        const suggestions = await autocomplete("declare function Test1() { } from Orders select |");
        expect(suggestions.find(x => x.value.startsWith("Test1")))
            .toBeTruthy();
    });
    
    it("doesn't complete declared function argument names in select", async () => {
        const suggestions = await autocomplete("declare function Test1(param1) { } from Orders select |");
        expect(suggestions.find(x => x.value.startsWith("param1")))
            .toBeFalsy();
    });
});
