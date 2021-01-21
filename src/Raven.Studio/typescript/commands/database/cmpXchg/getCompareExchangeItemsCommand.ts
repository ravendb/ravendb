import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getCompareExchangeItemsCommand extends commandBase {

    constructor(private database: database, private prefix: string, private start: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResult<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>> {
        const args = {
            start: this.start,
            pageSize: this.take,
            startsWith: this.prefix || undefined
        };

        const resultsSelector = (dto: resultsDto<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>) => {
            return {
                items: dto.Results,
                totalResultCount: -1
            } as pagedResult<Raven.Server.Web.System.CompareExchangeHandler.CompareExchangeListItem>;
        };
        const url = endpoints.databases.compareExchange.cmpxchg + this.urlEncodeArgs(args);
        return this.query(url, null, this.database, resultsSelector);
    }
}

export = getCompareExchangeItemsCommand;
