import queryCriteria = require("models/database/query/queryCriteria");
import database = require("models/resources/database");
import queryCommand = require("commands/database/query/queryCommand");

class docsIdsBasedOnQueryFetcher {

    static readonly MAX_RESULTS = 100;

    private database: KnockoutObservable<database>;

    constructor(database: KnockoutObservable<database>) {
        this.database = database
    }

    fetch(query: string, count: number = docsIdsBasedOnQueryFetcher.MAX_RESULTS): JQueryPromise<string[]> {
        if (!query)
            query = "from @all_docs";

        let wherelessQueryText = indexInfoQueryExtractor.extract(query);
        if (!wherelessQueryText)
            wherelessQueryText = "from @all_docs";
        
        const criteria = new queryCriteria();
        criteria.queryText(`${wherelessQueryText}`);
        criteria.metadataOnly(true);

        const q = new queryCommand(this.database(), 0, 100, criteria);
        return q.execute()
            .then(result => { 
                const ids = result.items
                .map(x => x.getId())
                .filter(x => x && x.indexOf('Raven') !== 0);

                return ids;
            });
    }
}

class indexInfoQueryExtractor {

    private static IndexInfoQueryPartRegex = /from\s+((index ('.+'|".+"))|([@_A-Za-z0-9]+))?/mi;

    static extract(query: string) {
        if (!query)
            return '';

        const m = query.match(indexInfoQueryExtractor.IndexInfoQueryPartRegex);
        return m ? m[0] : null;
    }
}

export = docsIdsBasedOnQueryFetcher;
