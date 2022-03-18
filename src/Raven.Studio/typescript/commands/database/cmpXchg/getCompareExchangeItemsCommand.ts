import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getCompareExchangeItemsCommand extends commandBase {

    constructor(private database: database, private prefix: string, private start: number, private take: number) {
        super();
    }

    execute(): JQueryPromise<pagedResult<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem>> {
        const args = {
            start: this.start,
            pageSize: this.take,
            startsWith: this.prefix || undefined
        };

        const resultsSelector = (dto: resultsDto<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem>): pagedResult<Raven.Server.Web.System.Processors.CompareExchange.CompareExchangeHandlerProcessorForGetCompareExchangeValues.CompareExchangeListItem> => {
            return {
                items: dto.Results,
                totalResultCount: -1
            };
        };
        const url = endpoints.databases.compareExchange.cmpxchg + this.urlEncodeArgs(args);
        return this.query(url, null, this.database, resultsSelector);
    }
}

export = getCompareExchangeItemsCommand;
