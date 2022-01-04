import { autocomplete } from "../autocompleteUtils";
import { AUTOCOMPLETE_META } from "../../src/providers/common";

describe("can complete limit", function () {
    it("doesn't complete fields in limit - first position", async () => {
        const suggestions = await autocomplete(" from Orders limit |");

        const fields = suggestions.filter(x => x.meta = AUTOCOMPLETE_META.field);
        expect(fields)
            .toHaveLength(0);
    });

    it("doesn't complete fields in limit - second position", async () => {
        const suggestions = await autocomplete(" from Orders limit 5, |");

        const fields = suggestions.filter(x => x.meta = AUTOCOMPLETE_META.field);
        expect(fields)
            .toHaveLength(0);
    });
    
    it("can suggest offset after limit provided", async () => {
        const suggestions = await autocomplete(" from Orders limit 5 |");

        const fields = suggestions.filter(x => x.meta === AUTOCOMPLETE_META.keyword && x.value.startsWith("offset"));
        expect(fields)
            .toHaveLength(1);
    });

    it("doesn't complete fields in offset", async () => {
        const suggestions = await autocomplete(" from Orders limit  5 offset |");

        const fields = suggestions.filter(x => x.meta = AUTOCOMPLETE_META.field);
        expect(fields)
            .toHaveLength(0);
    });
    
});
