import { autocomplete } from "../autocompleteUtils";
import { AUTOCOMPLETE_META } from "../../src/providers/common";

describe("can complete fields", function () {
    
    it("can complete in group by", async function () {
        const suggestions = await autocomplete("from Orders group by | ");

        expect(suggestions.map(x => x.caption))
            .toIncludeAllMembers(["Company", "Employee"]);
    });
    
    it("can complete partial in group by ", async function () {
        const suggestions = await autocomplete("from Orders group by Co|");
        expect(suggestions.map(x => x.caption))
            .toIncludeAllMembers(["Company"]);

        expect(suggestions.map(x => x.caption))
            .not.toIncludeAllMembers(["Employee"]);
    });

    it("can complete nested path in group by ", async () => {
        const suggestions = await autocomplete(" from Orders group by array(Lines[].|), ");

        expect(suggestions.map(x => x.caption))
            .toIncludeAllMembers(["PricePerUnit", "Discount"]);
    });
    
    it("can complete inner fields in group by - with spaces ", async function () {
        const suggestions = await autocomplete("from Orders group by Lines   .   |");
        expect(suggestions.map(x => x.caption))
            .toIncludeAllMembers(["PricePerUnit", "Discount"]);
    });

    it("can complete inner fields in group by - with out spaces ", async function () {
        const suggestions = await autocomplete("from Orders group by Lines.|  ");
        expect(suggestions.map(x => x.caption))
            .toIncludeAllMembers(["PricePerUnit", "Discount"]);
    });
    
    it("can complete after comma - root", async function () {
        const suggestions = await autocomplete("from Orders group by Field1, Lines.|  ");
        expect(suggestions.map(x => x.caption))
            .toIncludeAllMembers(["PricePerUnit", "Discount"]);
    });

    it("can complete after comma - inner", async function () {
        const suggestions = await autocomplete("from Orders group by Field1, |  ");
        expect(suggestions.map(x => x.caption))
            .toIncludeAllMembers(["Lines", "Company"]);
    });
    
    it("doesn't complete when comma is missing", async function () {
        const suggestions = await autocomplete("from Orders group by Field1 | ");
        
        // should contain keywords
        expect(suggestions.map(x => x.caption))
            .toIncludeAllMembers(["as", "limit", "select"]);
        
        // no fields
        expect(suggestions.filter(x => x.meta === AUTOCOMPLETE_META.field))
            .toHaveLength(0);
    });
});
