
//TODO: lock concurrent requests
class cachedMetadataProvider implements queryCompleterProviders {
    
    private readonly parent: queryCompleterProviders;
    
    private cachedIndexNames: string[] = undefined;
    private cachedCollections: string[] = undefined;
    private cachedIndexFields: Map<string, string[]> = new Map<string, string[]>();
    private cachedCollectionFields: Map<string, Map<string, dictionary<string>>> = new Map<string, Map<string, dictionary<string>>>();
    
    constructor(parent: queryCompleterProviders) {
        this.parent = parent;
    }

    collectionFields(collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void): void {
        if (!this.cachedCollectionFields.has(collectionName)) {
            this.cachedCollectionFields.set(collectionName, new Map<string, dictionary<string>>());
        }
        const collectionCache = this.cachedCollectionFields.get(collectionName);

        if (collectionCache.has(prefix)) {
            callback(collectionCache.get(prefix));
        } else {
            // no value in cache - call inner provider

            this.parent.collectionFields(collectionName, prefix, collectionFields => {
                collectionCache.set(prefix, collectionFields);
                callback(collectionFields);
            });
        }
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
        if (this.cachedIndexFields.has(indexName)) {
            const indexFields = this.cachedIndexFields.get(indexName);
            callback(indexFields);
        }
        
        this.parent.indexFields(indexName, indexFields => {
            this.cachedIndexFields.set(indexName, indexFields);
            callback(indexFields);
        });
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
