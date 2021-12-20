/**
 * This implementation communicates with main thread and fetches query completer information
 */
export class proxyMetadataProvider implements queryCompleterProviders {
    
    private readonly pendingMessages = new Map<number, Function>();
    private readonly sender: (payload: MetadataRequestPayload) => number;
    
    constructor(sender: (payload: MetadataRequestPayload) => number) {
        this.sender = sender;
    }

    collectionFields(collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void): void {
        const id = this.sender({
            type: "collectionFields",
            collectionName,
            prefix
        });
        
        this.pendingMessages.set(id, (payload: MetadataResponseCollectionFields) => callback(payload.fields));
    }

    collections(callback: (collectionNames: string[]) => void): void {
        const id = this.sender({
            type: "collections"
        });
        
        this.pendingMessages.set(id, (payload: MetadataResponseCollections) => callback(payload.names));
    }

    indexFields(indexName: string, callback: (fields: string[]) => void): void {
        const id = this.sender({
            type: "indexFields",
            indexName
        });
        
        this.pendingMessages.set(id, (payload: MetadataResponseIndexFields) => callback(payload.fields));
    }

    indexNames(callback: (indexNames: string[]) => void): void {
        const id = this.sender({
            type: "indexes"
        });
        
        this.pendingMessages.set(id, (payload: MetadataResponseIndexes) => callback(payload.names));
    }

    onResponse(id: number, response: MetadataResponsePayload) {
        const callback = this.pendingMessages.get(id);
        try {
            callback(response);
        } finally {
            this.pendingMessages.delete(id);
        }
    }
}
