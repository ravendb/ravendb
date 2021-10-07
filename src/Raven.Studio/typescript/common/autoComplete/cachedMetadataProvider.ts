
//TODO: lock concurrent requests
class cachedMetadataProvider implements queryCompleterProviders {
    
    private readonly parent: queryCompleterProviders;
    
    private cachedIndexNames: string[] = undefined;
    private cachedCollections: string[] = undefined;
    
    constructor(parent: queryCompleterProviders) {
        this.parent = parent;
    }

    collectionFields(collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void): void {
    }

    collections(callback: (collectionNames: string[]) => void): void {
        if (this.cachedCollections != null) {
            callback(this.cachedCollections);
        }
        
        this.parent.collections(names => {
            this.cachedCollections = names;
            callback(this.cachedCollections);
        });
    }

    indexFields(indexName: string, callback: (fields: string[]) => void): void {
    }

    indexNames(callback: (indexNames: string[]) => void): void {
        if (this.cachedIndexNames != null) {
            callback(this.cachedIndexNames);
        }
        
        this.parent.indexNames(names => {
            this.cachedIndexNames = names;
            callback(this.cachedIndexNames);
        });
    }

    terms(indexName: string, collection: string, field: string, pageSize: number, callback: (terms: string[]) => void): void {
    }
    
    
}

export = cachedMetadataProvider;
