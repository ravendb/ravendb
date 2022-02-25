import { autocomplete } from "../autocompleteUtils";
import { AUTOCOMPLETE_META } from "../../src/providers/common";

const nextKeywords = ["select", "limit", "load", "where", "filter"];

describe("can complete group by", function () {
    it("doesn't complete keywords as field name", async () => {
        const suggestions = await autocomplete(" from Orders group by |");

        for (const keyword of nextKeywords) {
            const matchingItem = suggestions.find(x => x.value.startsWith(keyword));
            expect(matchingItem)
                .toBeFalsy();
        }
    });
    
    it("doesn't complete keywords as nested field name", async () => {
        const suggestions = await autocomplete(" from Orders group by ShipTo.|");

        for (const keyword of nextKeywords) {
            const matchingItem = suggestions.find(x => x.value.startsWith(keyword));
            expect(matchingItem)
                .toBeFalsy();
        }
    });

    it("can complete Array() function in group by ", async () => {
        const suggestions = await autocomplete(" from Orders group by |");
        
        const arrayFunction = suggestions.find(x => x.value.startsWith("array("));
        expect(arrayFunction)
            .toBeTruthy();
        expect(arrayFunction.meta)
            .toEqual(AUTOCOMPLETE_META.function);
    });
    
    it("can complete next Array() function in group by ", async () => {
        const suggestions = await autocomplete(" from Orders group by array(Lines[].Product), |"); 

        const arrayFunction = suggestions.find(x => x.value.startsWith("array("));
        expect(arrayFunction)
            .toBeTruthy();
        expect(arrayFunction.meta)
            .toEqual(AUTOCOMPLETE_META.function);
    });
    
    it("can complete next keywords after group by ", async () => {
        const suggestions = await autocomplete(" from Orders group by Name |");

        for (const keyword of nextKeywords) {
            const matchingItem = suggestions.find(x => x.value.startsWith(keyword));
            expect(matchingItem)
                .toBeTruthy();
        }
    });
    
    it("doesn't suggest array inside array", async () => {
        const suggestions = await autocomplete(" from Orders group by Name, array(|");
        
        const arraySuggestion = suggestions.find(x => x.value.startsWith("array("));
        expect(arraySuggestion)
            .toBeFalsy();
    });
    
    it("doesn't suggest array after nested property in array", async () => {
        const suggestions = await autocomplete('from "Orders" group by array(Lines.|');

        const arraySuggestion = suggestions.find(x => x.value.startsWith("array("));
        expect(arraySuggestion)
            .toBeFalsy();
    });

    it("doesn't suggest array after field", async () => {
        const suggestions = await autocomplete(" from Orders group by Name |");

        const arraySuggestion = suggestions.find(x => x.value.startsWith("array("));
        expect(arraySuggestion)
            .toBeFalsy();
    });
});
