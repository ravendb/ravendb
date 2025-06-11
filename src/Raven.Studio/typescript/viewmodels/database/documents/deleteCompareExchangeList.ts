import database = require("models/resources/database");
import deleteCompareExchangeItemCommand = require("commands/database/cmpXchg/deleteCompareExchangeItemCommand");

type itemTypeDto = {
    Key: string;
    Index: number;
};

class deleteCompareExchangeList {
    constructor(
        private items: Array<itemTypeDto>,
        private db: database
    ) {
        this.db = db;
        this.items = items;
    }

    start(): Promise<Raven.Client.Documents.Operations.CompareExchange.CompareExchangeResult<any>[]> {
        if (this.items.length === 0) {
            return;
        }

        return Promise.all(
            this.items.map((item) => {
                return new deleteCompareExchangeItemCommand(this.db, item.Key, item.Index).execute();
            })
        );
    }
}

export = deleteCompareExchangeList;
