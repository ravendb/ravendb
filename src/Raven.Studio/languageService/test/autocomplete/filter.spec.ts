import { autocomplete } from "../autocompleteUtils";

const nextKeywords = ["select", "limit", "include"];

describe("can complete filter", function () {
    it("doesn't complete keywords as field name", async () => {
        const suggestions = await autocomplete(" from Orders filter |");

        for (const keyword of nextKeywords) {
            const matchingItem = suggestions.find(x => x.value.startsWith(keyword));
            expect(matchingItem)
                .toBeFalsy();
        }
    });
    
    it("doesn't complete keywords as nested field name", async () => {
        const suggestions = await autocomplete(" from Orders filter ShipTo.|");

        for (const keyword of nextKeywords) {
            const matchingItem = suggestions.find(x => x.value.startsWith(keyword));
            expect(matchingItem)
                .toBeFalsy();
        }
    });
    
    it("can complete next keywords after filter", async () => {
        const suggestions = await autocomplete(" from Orders filter Name == \"\" |");

        for (const keyword of nextKeywords) {
            const matchingItem = suggestions.find(x => x.value.startsWith(keyword));
            expect(matchingItem)
                .toBeTruthy();
        }
    });
    
    it("can complete filter_limit after filter", async () => {
        const suggestions = await autocomplete("from Orders filter Name == true |");
        
        const filterLimit = suggestions.find(x => x.caption === "filter_limit");
        expect(filterLimit)
            .toBeTruthy();
    });
    
    it("can't complete filter_limit when no filter", async () => {
        const suggestions = await autocomplete("from Orders  |");

        const filterLimit = suggestions.find(x => x.caption === "filter_limit");
        expect(filterLimit)
            .toBeFalsy();
    })
});
