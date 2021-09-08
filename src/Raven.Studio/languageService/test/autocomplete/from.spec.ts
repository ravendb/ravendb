import { autocomplete } from "../autocompleteUtils";

describe("can complete from", function () {

    it("empty", async function () {
        const suggestions = await autocomplete(" |");

        const captions = suggestions.map(x => x.caption);
        
        expect(captions)
            .toIncludeAllMembers(["from", "from index"]);
        expect(captions)
            .not.toIncludeAllMembers(["Orders"]);
    });
    
    it("partial", async function () {
        const suggestions = await autocomplete("fr|");
        
        const captions = suggestions.map(x => x.caption);
        
        expect(captions)
            .toIncludeAllMembers(["from"]);
        expect(captions)
            .not.toInclude("Orders");
    });
    
    describe("from collection", function () {
        it("can complete quoted collection - open", async function () {
            const suggestions = await autocomplete("from 'Ord|");
            
            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["Orders"]);
        });

        it("can complete quoted collection - closed", async function () {
            const suggestions = await autocomplete("from 'Ord|'");
            
            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["Orders"]);
        });
        
        it("can complete collection name with where", async function () {
            const suggestions = await autocomplete("from Ord| where ");

            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["Orders"]);
        });

        it("doesn't provide collection is already provided", async function () {
            const suggestions = await autocomplete("from Orders | ");

            expect(suggestions.map(x => x.caption))
                .not.toIncludeAllMembers(["Orders"]);
        });
    });
    
    describe("from index", function () {
        it("can complete index name", async function () {
            const suggestions = await autocomplete(`from
             index "Product|`);
            console.log(suggestions); //TODO:
        });
        
        //TODO: from index Produc --> add quotes
    });
})
