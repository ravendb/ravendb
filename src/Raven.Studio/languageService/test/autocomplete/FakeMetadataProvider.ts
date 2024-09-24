

function stubCollectionFields(): Map<string, Map<string, dictionary<string>>> {
    const result = new Map<string, Map<string, dictionary<string>>>();
    
    const insert = (collection: string, prefix: string, values: dictionary<string>) => {
        if (!result.has(collection)) {
            result.set(collection, new Map<string, dictionary<string>>());
        }
        
        const perCollection = result.get(collection);
        perCollection.set(prefix, values);
    };
    
    insert("Orders", "", {
        Company: "String",
        Employee: "String",
        Freight: "Number",
        Lines: "ArrayObject",
        OrderedAt: "String",
        ShipTo: "Object",
        ShipVia: "String",
        ["id()"]: "String"
    });
    
    insert("Orders", "Lines", {
        Discount: "Number",
        PricePerUnit: "Number",
        Product: "String",
        ProductName: "String",
        Quantity: "Number"
    })
    
    insert("Products", "", {
        Category: "String",
        Discontinued: "Boolean",
        Name: "String",
        PricePerUnit: "Number",
        QuantityPerUnit: "String",
        Supplier: "String",
        ["id()"]: "String"
    });
    
    return result;
}

function stubIndexFields(): Map<string, string[]> {
    const result = new Map<string, string[]>();
    result.set("Orders/ByCompany", ["Company", "Count", "Total", "id()"]);
    result.set("Product/Rating", ["Name", "Rating", "TotalVotes", "AllRatings"]);
    result.set("Orders/Totals", ["Employee", "Company", "Total"]);
    return result;
}

export class FakeMetadataProvider implements queryCompleterProviders {
    
    private collectionStubs = stubCollectionFields();
    private indexStubs = stubIndexFields();
    
    public constructor(overrides?: { indexes?: Record<string, string[]>, collections?: Record<string, Record<string, Record<string, string>>> }) {
        if (overrides?.indexes) {
            this.indexStubs = new Map<string, string[]>(Object.entries(overrides.indexes ?? []));
        }
        if (overrides?.collections) {
            const map = new Map<string, Map<string, dictionary<string>>>();
            for (const [key, value] of Object.entries(overrides.collections ?? [])) {
                map.set(key, new Map<string, dictionary<string>>(Object.entries(value ?? [])));
            }
            this.collectionStubs = map;
        }
    }
    
    indexNames(callback: (indexNames: string[]) => void) {
        callback(Array.from(this.indexStubs.keys()));
    }
    
    collections(callback: (collections: string[]) => void) {
        callback(Array.from(this.collectionStubs.keys()));
    }

    collectionFields(collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void): void {
        if (this.collectionStubs.has(collectionName)) {
            const perCollectionInfo = this.collectionStubs.get(collectionName);
            
            callback(perCollectionInfo.get(prefix) || {});
        } else {
            callback({});
        }
    }

    indexFields(indexName: string, callback: (fields: string[]) => void): void {
        if (this.indexStubs.has(indexName)) {
            callback(this.indexStubs.get(indexName));
        } else {
            callback([]);
        }
    }
}
