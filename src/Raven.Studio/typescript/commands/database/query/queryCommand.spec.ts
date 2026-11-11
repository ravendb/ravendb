require("knockout.validation");
import queryCommand = require("commands/database/query/queryCommand");
import queryCriteria = require("models/database/query/queryCriteria");
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

interface ServerResponse {
    status: 200 | 304;
    etag?: string;
    requestTime?: string;
    body?: Partial<Raven.Client.Documents.Queries.QueryResult<any[], any>>;
}

describe("queryCommand", () => {
    const databaseName = DatabasesStubs.nonShardedSingleNodeDatabase().name;
    const ordersResult = { Results: [{ Company: "companies/1-A" }], TotalResults: 1, DurationInMs: 120, ResultEtag: 0 };

    let ajax: jest.SpyInstance;

    function mockServer(...responses: ServerResponse[]) {
        ajax = jest.spyOn($, "ajax");
        responses.forEach(({ status, etag, requestTime, body }) =>
            ajax.mockImplementationOnce(() =>
                $.Deferred()
                    .resolve(body, status === 304 ? "notmodified" : "success", {
                        status,
                        responseText: JSON.stringify(body),
                        getResponseHeader: (name: string) => (name === "Request-Time" ? requestTime : etag),
                    })
                    .promise()
            )
        );
    }

    function runQuery(queryText: string, cacheEnabled = true) {
        const criteria = queryCriteria.empty();
        criteria.queryText(queryText);
        return new queryCommand({ db: databaseName, skip: 0, take: 25, criteria, cacheEnabled }).execute();
    }

    function sentEtags() {
        return ajax.mock.calls.map(([settings]: [JQueryAjaxSettings]) => settings.headers["If-None-Match"]);
    }

    afterEach(() => {
        ajax.mockRestore();
    });

    it("reuses the previous results when the server answers 304", async () => {
        mockServer({ status: 200, etag: '"5"', body: ordersResult }, { status: 304, requestTime: "3" });

        const first = await runQuery("from Orders where Freight > 10");
        const second = await runQuery("from Orders where Freight > 10");

        expect(sentEtags()).toEqual([undefined, "5"]);
        expect(first.fromCache).toBeFalse();
        expect(second.fromCache).toBeTrue();
        expect(second.items.map((x) => x.toDto())).toEqual(ordersResult.Results);
        expect(second.additionalResultInfo.DurationInMs).toBe(3);
    });

    it("does not send the etag when the cache is disabled", async () => {
        mockServer({ status: 200, etag: "6", body: ordersResult }, { status: 200, etag: "6", body: ordersResult });

        await runQuery("from Orders where Freight > 20");
        const second = await runQuery("from Orders where Freight > 20", false);

        expect(sentEtags()).toEqual([undefined, undefined]);
        expect(second.fromCache).toBeFalse();
    });

    it("reuses results fetched with the cache disabled once the cache is enabled", async () => {
        mockServer({ status: 200, etag: "9", body: ordersResult }, { status: 304, requestTime: "2" });

        await runQuery("from Orders where Freight > 40", false);
        const second = await runQuery("from Orders where Freight > 40");

        expect(sentEtags()).toEqual([undefined, "9"]);
        expect(second.fromCache).toBeTrue();
        expect(second.items.map((x) => x.toDto())).toEqual(ordersResult.Results);
    });

    it("does not send the etag of a different query", async () => {
        mockServer({ status: 200, etag: "7", body: ordersResult }, { status: 200, etag: "8", body: ordersResult });

        await runQuery("from Orders where Freight > 30");
        await runQuery("from Orders where Freight > 31");

        expect(sentEtags()).toEqual([undefined, undefined]);
    });
});
