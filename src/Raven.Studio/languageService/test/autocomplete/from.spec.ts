import { autocomplete } from "../autocompleteUtils";
import { META_COLLECTION } from "../../src/providers/scoring";

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
        
        it("can complete @all_docs", async function() {
            const suggestions = await autocomplete("from |");
            const allDocs = suggestions.find(x => x.value.includes("@all_docs"));
            expect(allDocs)
                .toBeTruthy();
            
            expect(allDocs.meta)
                .toEqual(META_COLLECTION);
        });
        
        it("can complete collection", async function () {
            const suggestions = await autocomplete("from Orders| ");

            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["Orders"]);
        });
        
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

        it("can complete inside", async function () {
            const suggestions = await autocomplete("from Ord|ers  ");

            expect(suggestions.map(x => x.caption))
                .toIncludeAllMembers(["Orders"]);
        });
        
        it("doesn't suggest special function in alias", async function() {
            const suggestions = await autocomplete("from Orders as |");
            
            const fuzzy = suggestions.find(x => x.value.includes("fuzzy"));
            expect(fuzzy)
                .toBeFalsy();

            const search = suggestions.find(x => x.value.includes("search"));
            expect(search)
                .toBeFalsy();
        })
    });
    
    describe("from index", function () {
        it("doesn't repeat last token", async function () {
            const suggestions = await autocomplete(`from index|`);
            expect(suggestions.map(x => x.caption))
                .not.toIncludeAllMembers(["index"]);
        });
        
        it("can complete index name - no index yet defined", async function () {
            const suggestions = await autocomplete(`from index |`);
            expect(suggestions.map(x => x.value))
                .toIncludeAllMembers(['"Orders/ByCompany" ']);
        });
        
        it("can complete index name - when open double quote", async function () {
            const suggestions = await autocomplete(`from index "Orde|`);
            expect(suggestions.map(x => x.value))
                .toIncludeAllMembers(['"Orders/ByCompany" ']);
        });

        it("can complete index name - when open single quote", async function () {
            const suggestions = await autocomplete(`from index 'Orde|`);
            expect(suggestions.map(x => x.value))
                .toIncludeAllMembers(["'Orders/ByCompany' "]);
        });

        it("can complete index name - when inside double quote", async function () {
            const suggestions = await autocomplete(`from index "Orde|"`);
            expect(suggestions.map(x => x.value))
                .toIncludeAllMembers(['"Orders/ByCompany" ']);
        });
        
        it("can complete index name - when inside single quote", async function () {
            const suggestions = await autocomplete(`from index 'Orde|'`);
            expect(suggestions.map(x => x.value))
                .toIncludeAllMembers(["'Orders/ByCompany' "]);
        });
        
        it("can complete index name - when where exists", async function () {
            const suggestions = await autocomplete(`from index "| where`);
            expect(suggestions.map(x => x.value))
                .toIncludeAllMembers(['"Orders/ByCompany" ']);
            expect(suggestions.map(x => x.value))
                .not.toIncludeAllMembers(["Employees"]);
        });
    });
    
    describe("with describe", function () {
        it("can complete from after declare function - w/o from ahead", async function () {
            const suggestions = await autocomplete("declare function Name() { \r\n\r\n } | ");

            const from = suggestions.find(x => x.value.startsWith("from"));
            expect(from)
                .toBeTruthy();
            
            const where = suggestions.find(x => x.value.startsWith("where"));
            expect(where)
                .toBeFalsy();
        });
        
        it("can complete from after declare function - with from ahead", async function () {
            const suggestions = await autocomplete("declare function Name() { \r\n\r\n } |\r\nfrom Orders ");
            
            const from = suggestions.find(x => x.value.startsWith("from"));
            expect(from)
                .toBeTruthy();

            const where = suggestions.find(x => x.value.startsWith("where"));
            expect(where)
                .toBeFalsy();
        });
    });
    
    describe("alias", function () {
        it("has empty list when entering as alias", async function () {
            const suggestions = await autocomplete(`from Orders as |`);
            
            expect(suggestions)
                .toHaveLength(0);
        });
        
        it("can complete keywords when no alias yet defined", async function () {
            const suggestions = await autocomplete(`from Orders |`);
            
            const values = suggestions.map(x => x.value);
            expect(values)
                .toContain("as ");
            
            const whereSuggestion = suggestions.find(x => x.value.startsWith("where"));
            expect(whereSuggestion)
                .toBeTruthy();
        });

        it("can complete keywords when alias was defined", async function () {
            const suggestions = await autocomplete(`from Orders as o |`);

            const whereSuggestion = suggestions.find(x => x.value.startsWith("where"));
            expect(whereSuggestion)
                .toBeTruthy();
        });
    });
})
