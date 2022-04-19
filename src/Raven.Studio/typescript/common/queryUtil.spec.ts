import queryUtil from "common/queryUtil";

describe("queryUtil", function () {
    describe("replaceSelectAndIncludeWithFetchAllStoredFields", function () {
        
        const act = (input: string) => queryUtil.replaceSelectAndIncludeWithFetchAllStoredFields(input);
        
        it("can append select", () => {
            const result = act("from Orders");

            expect(result)
                .toEqual("from Orders select __all_stored_fields");
        });
        
        it("can replace select stmt", () => {
            const result = act("from People select FirstName, LastName");
            
            expect(result)
                .toEqual("from People select __all_stored_fields");
        });
        
        it("can work with index and limit", () => {
            const result = act("from index 'Product/Search' limit 5");
            
            expect(result)
                .toEqual("from index 'Product/Search' select __all_stored_fields limit 5");
        });

        it("can work with index and include", () => {
            const result = act("from index 'Orders/ByCompany' select Count include Seller");

            expect(result)
                .toEqual("from index 'Orders/ByCompany' select __all_stored_fields include Seller");
        });
        
        it("can replace js select", () => {
            const result = act("from Orders as o select { Name: o.Name }");
            expect(result)
                .toEqual("from Orders as o select __all_stored_fields");
        });
    });
    
    
    describe("getCollectionOrIndexName", function () {
        const act = (query: string) => queryUtil.getCollectionOrIndexName(query);
        
        it("can get from collection", () => {
            const [source, type] = act("from Orders");
            expect(type)
                .toEqual("collection");

            expect(source)
                .toEqual("Orders");
        });
        
        it("can get from index", () => {
            const [source, type] = act("from index 'Products/Search'");
            expect(type)
                .toEqual("index");
            expect(source)
                .toEqual("Products/Search");
        });
        
        it("can handle unknown", () => {
            const [source, type] = act("limit ");
            expect(type)
                .toEqual("unknown");
            expect(source)
                .toBeUndefined();
        })
    });
    
    describe("isDynamicQuery", function () {
        const act = (query: string) => queryUtil.isDynamicQuery(query);
        
        it("from collection is dynamic", () => {
            const result = act("from Orders");
            expect(result)
                .toBeTrue();
        });
        
        it("from index is not dynamic", () => {
            const result = act("from index 'Products/Search'");
            expect(result)
                .toBeFalse();
        });
    });

    describe("escapeName", function () {
        const act = (name: string) => queryUtil.escapeName(name);

        it("adds quotes on simple names", () => {
            const result = act("name");
            expect(result)
                .toEqual("'name'");
        });

        it("escapes single quotes", () => {
            const result = act("it'squoted");
            expect(result)
                .toEqual("'it''squoted'");
        });

        it("escapes the escape char", () => {
            const result = act("itcontains\\literal");
            expect(result)
                .toEqual("'itcontains\\\\literal'");
        });
    });
});
