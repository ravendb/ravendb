
export class EmptyMetadataProvider implements queryCompleterProviders {
    
    indexNames(callback: (indexNames: string[]) => void) {
        callback([]);
    }
    
    collections(callback: (collections: string[]) => void) {
        callback([]);
    }

    collectionFields(collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void): void {
        callback({});
    }

    indexFields(indexName: string, callback: (fields: string[]) => void): void {
        callback([]);
    }
}
