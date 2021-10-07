

const indexes = ['Orders/ByCompany', 'Product/Rating'];
const collections = ["Orders", "Products", "Employees"];

export class FakeMetadataProvider implements queryCompleterProviders {
    indexNames(callback: (indexNames: string[]) => void) {
        callback(indexes);
    }
    
    collections(callback: (collections: string[]) => void) {
        callback(collections);
    }

    collectionFields(collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void): void {
        callback({}); //TODO:
    }

    indexFields(indexName: string, callback: (fields: string[]) => void): void {
        callback([]); //tODO:
    }

    terms(indexName: string, collection: string, field: string, pageSize: number, callback: (terms: string[]) => void): void {
        callback([]); //TODO:
    }
    
    
    
}
