

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
        ShipVia: "String"
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
    });
    
    return result;
}

const indexes = ['Orders/ByCompany', 'Product/Rating'];


export class FakeMetadataProvider implements queryCompleterProviders {
    
    private collectionStubs = stubCollectionFields();
    
    indexNames(callback: (indexNames: string[]) => void) {
        callback(indexes);
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
        callback([]); //tODO:
    }

    terms(indexName: string, collection: string, field: string, pageSize: number, callback: (terms: string[]) => void): void {
        callback([]); //TODO:
    }
    
}
