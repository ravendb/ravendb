
class cachedMetadataProvider implements queryCompleterProviders {
    
    private readonly parent: queryCompleterProviders;
    
    private cachedIndexNames: Promise<string[]> = undefined;
    private cachedCollections: Promise<string[]> = undefined;
    private cachedIndexFields: Map<string, Promise<string[]>> = new Map<string, Promise<string[]>>();
    private cachedCollectionFields: Map<string, Map<string, Promise<dictionary<string>>>> = new Map<string, Map<string, Promise<dictionary<string>>>>();
    
    constructor(parent: queryCompleterProviders) {
        this.parent = parent;
    }

    async collectionFields(collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void) {
        if (!this.cachedCollectionFields.has(collectionName)) {
            this.cachedCollectionFields.set(collectionName, new Map<string, Promise<dictionary<string>>>());
        }
        const collectionCache = this.cachedCollectionFields.get(collectionName);

        if (collectionCache.has(prefix)) {
            const fields = await collectionCache.get(prefix);
            callback(fields);
        } else {
            // no value in cache - call inner provider
            const task = new Promise<dictionary<string>>(resolve => {
                this.parent.collectionFields(collectionName, prefix, resolve);
            });
            
            collectionCache.set(prefix, task);
            
            await this.collectionFields(collectionName, prefix, callback);
        }
    }

    async collections(callback: (collectionNames: string[]) => void) {
        if (this.cachedCollections) {
            const collections = await this.cachedCollections;
            callback(collections);
        } else {
            this.cachedCollections = new Promise<string[]>(resolve => {
                this.parent.collections(resolve);
            });
            
            await this.collections(callback);
        }
    }

    async indexFields(indexName: string, callback: (fields: string[]) => void) {
        if (this.cachedIndexFields.has(indexName)) {
            const indexFields = await this.cachedIndexFields.get(indexName);
            callback(indexFields);
        } else {
            const task = new Promise<string[]>(resolve => {
                this.parent.indexFields(indexName, resolve);
            });

            this.cachedIndexFields.set(indexName, task);

            await this.indexFields(indexName, callback);
        }
    }

    async indexNames(callback: (indexNames: string[]) => void) {
        if (this.cachedIndexNames) {
            const indexNames = await this.cachedIndexNames;
            callback(indexNames);
        } else {
            this.cachedIndexNames = new Promise<string[]>(resolve => {
                this.parent.indexNames(resolve);
            });
            
            await this.indexNames(callback);
        }
    }
}

export = cachedMetadataProvider;
