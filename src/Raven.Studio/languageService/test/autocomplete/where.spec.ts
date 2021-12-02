import {autocomplete} from "../autocompleteUtils";
import {AUTOCOMPLETE_META} from "../../src/providers/common";
import {EmptyMetadataProvider} from "./EmptyMetadataProvider";

const specialFunctions = [
    "fuzzy", "search", "facet", "boost", "startsWith", "lucene", "exists",
    "endsWith", "moreLikeThis", "intersect", "exact", "regex", "proximity"
];

describe("can complete where", function () {
    it("can complete special functions - collection has fields", async () => {
        const suggestions = await autocomplete("from Orders where |");

        for (let specialFunction of specialFunctions) {
            const matchingItem = suggestions.find(x => x.value.startsWith(specialFunction + "("));
            expect(matchingItem)
                .toBeTruthy();
        }
    });

    it("can complete special functions - collection doesn't have fields", async () => {
        const suggestions = await autocomplete("from CollectionWithoutDefinedFields where |", new EmptyMetadataProvider());

        for (let specialFunction of specialFunctions) {
            const matchingItem = suggestions.find(x => x.value.startsWith(specialFunction + "("));
            expect(matchingItem)
                .toBeTruthy();
        }
    });
    
    it("can complete fields - at root level", async () => {
        const suggestions = await autocomplete("from Orders where |");
        
        const companyField = suggestions.find(x => x.value.startsWith("Company") && x.meta === AUTOCOMPLETE_META.field);
        expect(companyField)
            .toBeTruthy();
    });

    it("can complete fields - inside special functions", async () => {
        const suggestions = await autocomplete("from Orders where exact(|");

        const companyField = suggestions.find(x => x.value.startsWith("Company") && x.meta === AUTOCOMPLETE_META.field);
        expect(companyField)
            .toBeTruthy();
    });
    
    it("doesn't complete functions inside functions", async () => {
        const suggestions = await autocomplete("from Orders where search(|");

        for (let specialFunction of specialFunctions) {
            const matchingItem = suggestions.find(x => x.value.startsWith(specialFunction + "("));
            expect(matchingItem)
                .toBeFalsy();
        }
    });
    
    it("doesn't complete root keywords inside special functions", async () => {
        const suggestions = await autocomplete("from Orders where search(|");

        const keywords = ["select", "from", "where", "limit"];
        
        for (let keyword of keywords) {
            const matchingItem = suggestions.find(x => x.value.startsWith(keyword));
            expect(matchingItem)
                .toBeFalsy();
        }
    });
    
    it("doesn't complete suggest in where", async () => {
        const suggestions = await autocomplete("from Orders where |");
        
        expect(suggestions.find(x => x.value.startsWith("suggest(")))
            .toBeFalsy();
    });
    
    describe("and / or ", function () {
        it("can complete and/or in where after predicate", async () => {
            const suggestions = await autocomplete("from Orders where Name == 'Test1' | ");
            
            const orSuggestion = suggestions.find(x => x.value === "or ");
            expect(orSuggestion)
                .toBeTruthy();
            expect(orSuggestion.meta)
                .toEqual(AUTOCOMPLETE_META.operator);

            const andSuggestion = suggestions.find(x => x.value === "and ");
            expect(andSuggestion)
                .toBeTruthy();
            expect(andSuggestion.meta)
                .toEqual(AUTOCOMPLETE_META.operator);

        });
    });
    
    describe("operators", function () {
        it("can complete math operators", async () => {
            const suggestions = await autocomplete("from Orders where Name |");
            
            expect(suggestions.find(x => x.value === "=="))
                .toBeTruthy();
            expect(suggestions.find(x => x.value === "!="))
                .toBeTruthy();
            expect(suggestions.find(x => x.value === "<>"))
                .toBeTruthy();
            expect(suggestions.find(x => x.value === "<"))
                .toBeTruthy();
            expect(suggestions.find(x => x.value === "<="))
                .toBeTruthy();
            expect(suggestions.find(x => x.value === ">"))
                .toBeTruthy();
            expect(suggestions.find(x => x.value === ">="))
                .toBeTruthy();
        })
    })
});
