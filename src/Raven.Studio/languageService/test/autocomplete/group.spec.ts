import { autocomplete } from "../autocompleteUtils";
import { META_FUNCTION } from "../../src/providers/scoring";

const nextKeywords = ["select", "limit", "load", "where"];

describe("can complete from", function () {
    it("doesn't complete keywords as field name", async () => {
        const suggestions = await autocomplete(" from Orders group by |");

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
            .toEqual(META_FUNCTION);
    });
    
    it("can complete next Array() function in group by ", async () => {
        const suggestions = await autocomplete(" from Orders group by array(Lines[].Product), |"); 

        const arrayFunction = suggestions.find(x => x.value.startsWith("array("));
        expect(arrayFunction)
            .toBeTruthy();
        expect(arrayFunction.meta)
            .toEqual(META_FUNCTION);
    });

    it("can complete nested path in group by ", async () => {
        const suggestions = await autocomplete(" from Orders group by array(Lines[].|), ");

        const arrayFunction = suggestions.find(x => x.value.startsWith("array("));
        expect(arrayFunction)
            .toBeTruthy();
        expect(arrayFunction.meta)
            .toEqual(META_FUNCTION);
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
});
