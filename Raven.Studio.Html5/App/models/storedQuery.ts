import querySort = require("models/querySort");
import database = require("models/database");

class storedQuery {

    isPinned = ko.observable(false);
    id: string;

    constructor(public queryText: string, public sorts: querySort[], public databaseName: string, public transformerName: string, public showFields: boolean, public indexEntries: boolean, public useAndOperator: boolean) {
        
    }

    static getAll(db: database): storedQuery[]{
        var storage = window.localStorage;
        if (storage) {
            var storedQueryIds: string[] = storage.getItem("storedQueryList_" + db.name) || [];
            return storedQueryIds
                .map(id => storage.getItem(id))
                .map((json: string) => JSON.parse(json))
                .map((dto: storedQueryDto) => storedQuery.fromDto(dto))
                .sort((a, b) => a.isPinned() && !b.isPinned() ? -1 : b.isPinned() && !a.isPinned() ? 1 : 0);
        }

        return [];
    }

    save() {
        var storage = window.localStorage;
        if (storage) {
            if (this.id) {
                // TODO: continue here.
            }
        }
    }

    static fromDto(dto: storedQueryDto): storedQuery {
        var sorts = dto.sorts.map(s => querySort.fromQuerySortString(s));
        return new storedQuery(dto.queryText, sorts, dto.databaseName, dto.transformerName, dto.showFields, dto.indexEntries, dto.useAndOperator);
    }

    toDto(): storedQueryDto {
        return {
            databaseName: this.databaseName,
            id: this.id,
            indexEntries: this.indexEntries,
            isPinned: this.isPinned(),
            queryText: this.queryText,
            showFields: this.showFields,
            sorts: this.sorts.map(s => s.toQuerySortString()),
            transformerName: this.transformerName,
            useAndOperator: this.useAndOperator
        };
    }
}

export = storedQuery;