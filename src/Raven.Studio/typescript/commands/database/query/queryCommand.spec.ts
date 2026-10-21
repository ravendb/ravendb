require("knockout.validation");
import queryCommand = require("commands/database/query/queryCommand");
import queryCriteria = require("models/database/query/queryCriteria");
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

interface ServerResponse {
    status: 200 | 304;
    etag?: string;
    body?: Partial<Raven.Client.Documents.Queries.QueryResult<any[], any>>;
}

describe("queryCommand", () => {
    const databaseName = DatabasesStubs.nonShardedSingleNodeDatabase().name;
    const ordersResult = { Results: [{ Company: "companies/1-A" }], TotalResults: 1, DurationInMs: 120, ResultEtag: 0 };

    let ajax: jest.SpyInstance;

    function mockServer(...responses: ServerResponse[]) {
        ajax = jest.spyOn($, "ajax");
        responses.forEach(({ status, etag, body }) =>
            ajax.mockImplementationOnce(() =>
                $.Deferred()
                    .resolve(body, status === 304 ? "notmodified" : "success", {
                        status,
                        getResponseHeader: () => etag,
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
        mockServer({ status: 200, etag: '"5"', body: ordersResult }, { status: 304 });

        const first = await runQuery("from Orders where Freight > 10");
        first.additionalResultInfo.DurationInMs = 1;
        const second = await runQuery("from Orders where Freight > 10");

        expect(sentEtags()).toEqual([undefined, "5"]);
        expect(first.fromCache).toBeFalse();
        expect(second.fromCache).toBeTrue();
        expect(second.items.map((x) => x.toDto())).toEqual(ordersResult.Results);
        expect(second.additionalResultInfo.DurationInMs).toBe(120);
    });

    it("does not send the etag when the cache is disabled", async () => {
        mockServer({ status: 200, etag: "6", body: ordersResult }, { status: 200, etag: "6", body: ordersResult });

        await runQuery("from Orders where Freight > 20", false);
        const second = await runQuery("from Orders where Freight > 20", false);

        expect(sentEtags()).toEqual([undefined, undefined]);
        expect(second.fromCache).toBeFalse();
    });

    it("does not send the etag of a different query", async () => {
        mockServer({ status: 200, etag: "7", body: ordersResult }, { status: 200, etag: "8", body: ordersResult });

        await runQuery("from Orders where Freight > 30");
        await runQuery("from Orders where Freight > 31");

        expect(sentEtags()).toEqual([undefined, undefined]);
    });
});
