

const indexes = ['Orders/ByCompany', 'Product/Rating'];
const collections = ["Orders", "Products", "Employees"];

export class FakeMetadataProvider implements queryCompleterProviders {
    indexNames(callback: (indexNames: string[]) => void) {
        callback(indexes);
    }
    
    terms() {
        //TODO:
    }
    
    collections(callback: (collections: string[]) => void) {
        callback(collections);
    }
    
    collectionFields() {
        //TODO:
    }
    
    indexFields() {
        //TODO:
    }
}
