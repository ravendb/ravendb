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
        throw new Error("not implemented");
    }

    collections(callback: (collectionNames: string[]) => void): void {
        const id = this.sender({
            type: "collections"
        });
        
        this.pendingMessages.set(id, (payload: MetadataResponseCollections) => callback(payload.names));
    }

    indexFields(indexName: string, callback: (fields: string[]) => void): void {
        throw new Error("not implemented");
    }

    indexNames(callback: (indexNames: string[]) => void): void {
        const id = this.sender({
            type: "indexes"
        });
        
        this.pendingMessages.set(id, (payload: MetadataResponseIndexes) => callback(payload.names));
    }

    terms(indexName: string, collection: string, field: string, pageSize: number, callback: (terms: string[]) => void): void {
        throw new Error("not implemented");
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
