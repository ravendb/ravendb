import { autocomplete } from "../autocompleteUtils";
import { AUTOCOMPLETE_META } from "../../src/providers/common";

describe("can complete include", function () {
    it("doesn't complete keywords as field name", async () => {
        const suggestions = await autocomplete(" from Orders include |");

        const keywords = ["select", "limit", "load", "where"];
        
        for (const keyword of keywords) {
            const matchingItem = suggestions.find(x => x.value.startsWith(keyword));
            expect(matchingItem)
                .toBeFalsy();
        }
    });
    
    it("can complete field", async () => {
        const suggestions = await autocomplete(" from Orders include |"); 

        const arrayFunction = suggestions.find(x => x.value.startsWith("Lines"));
        expect(arrayFunction)
            .toBeTruthy();
        expect(arrayFunction.meta)
            .toEqual(AUTOCOMPLETE_META.field);
    });

    it("can complete next field", async () => {
        const suggestions = await autocomplete(" from Orders include Employee, |");

        const arrayFunction = suggestions.find(x => x.value.startsWith("Lines"));
        expect(arrayFunction)
            .toBeTruthy();
        expect(arrayFunction.meta)
            .toEqual(AUTOCOMPLETE_META.field);
    });
});
