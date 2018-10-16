import queryCriteria = require("models/database/query/queryCriteria");
import database = require("models/resources/database");
import queryCommand = require("commands/database/query/queryCommand");

class docsIdsBasedOnQueryFetcher {

    static readonly MAX_RESULTS = 100;

    private database: KnockoutObservable<database>;

    constructor(database: KnockoutObservable<database>) {
        this.database = database
    }
    
    fetch(documentIdPrefix: string, query: string, count: number = docsIdsBasedOnQueryFetcher.MAX_RESULTS): JQueryPromise<string[]> {
        if (!query)
            query = "from @all_docs";

        let wherelessQueryText = indexInfoQueryExtractor.extract(query);
        if (!wherelessQueryText)
            wherelessQueryText = "from @all_docs";
        
        const criteria = new queryCriteria();
        criteria.queryText(`${wherelessQueryText} where startsWith(id(), '${this.escape(documentIdPrefix)}')`);
        criteria.metadataOnly(true);
        
        return new queryCommand(this.database(), 0, 10, criteria)
            .execute()
            .then(result => result.items.map(x => x.getId()));
    }

    private escape(inputString: string) {
        return inputString ? inputString.replace("'", "''") : "";
    }
}

class indexInfoQueryExtractor {

    /**
     * Should match query w/o where statement
     *     
     * from index 'Products/Search' where "Dfd"
from index Second where
from Order where
from 'Roder' where

from index 'Products/Search'
from index Second
from Order
from 'Roder'

from index "Products/Search"
from index Second
from "Order"
from 'Roder'

     * @type {RegExp}
     */
    private static IndexInfoQueryPartRegex = /from\s+(index)?\s*(('[^']+'|"[^"]+")|[\S]+)/mi;
    
    static extract(query: string) {
        if (!query)
            return '';

        const m = query.match(indexInfoQueryExtractor.IndexInfoQueryPartRegex);
        return m ? m[0] : null;
    }
}

export = docsIdsBasedOnQueryFetcher;
