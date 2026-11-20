require("knockout.validation");
import query = require("viewmodels/database/query/query");
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

jest.mock("views/database/query/query.html", () => ({ default: "<div></div>" }));
jest.mock("views/partial/columnsSelector.html", () => ({ default: "<div></div>" }));
jest.mock("common/bindingHelpers/datePickerBindingHandler");

ko.DirtyFlag = require("external/dirtyFlag").DirtyFlag;

type QueryResult = Partial<Raven.Client.Documents.Queries.QueryResult<any[], any>>;

describe("query", () => {
    const gridFirstPageTake = 100;
    const gridNextPageTake = 30;

    let ajax: jest.SpyInstance;

    function ordersResponse(resultsCount: number, skippedResults: number): QueryResult {
        return {
            Results: Array.from({ length: resultsCount }, (_item, index) => ({ Company: `companies/${index}-A` })),
            TotalResults: 830,
            SkippedResults: skippedResults,
            IndexName: "collection/Orders",
            ResultEtag: 0,
        };
    }

    function resolveWith(response: JQueryDeferred<any>, body: QueryResult) {
        response.resolve(body, "success", {
            status: 200,
            responseText: JSON.stringify(body),
            getResponseHeader: (): string => null,
        });
    }

    function mockServer(...bodies: QueryResult[]) {
        bodies.forEach((body) =>
            ajax.mockImplementationOnce(() => {
                const response = $.Deferred();
                resolveWith(response, body);
                return response.promise();
            })
        );
    }

    function mockPendingResponse() {
        const response = $.Deferred();
        ajax.mockImplementationOnce(() => response.promise());
        return response;
    }

    function sentStarts(): number[] {
        return ajax.mock.calls.map(([settings]: [JQueryAjaxSettings]) => JSON.parse(settings.data as string).Start);
    }

    function openQueryPage(queryText: string) {
        const page = new query(DatabasesStubs.nonShardedSingleNodeDatabase());
        let lastFetch: JQueryPromise<unknown>;

        const fetchRows = (skip: number, take: number) => {
            lastFetch = page.effectiveFetcher()(skip, take);
            return lastFetch;
        };
        const grid = { reset: () => fetchRows(0, gridFirstPageTake) };

        page["gridController"](grid as any);
        page.queryFetcher.subscribe(() => grid.reset());
        page.criteria().queryText(queryText);

        return {
            page,
            runQuery: () => {
                page.runQuery();
                return lastFetch;
            },
            goToResultsTab: () => {
                page.goToResultsTab();
                return lastFetch;
            },
            scrollToNextPage: (loadedRowsCount: number) => fetchRows(loadedRowsCount, gridNextPageTake),
        };
    }

    beforeEach(() => {
        ajax = jest.spyOn($, "ajax");
    });

    afterEach(() => {
        ajax.mockRestore();
    });

    it("adds SkippedResults of every loaded page to Start of the next page", async () => {
        mockServer(ordersResponse(101, 29), ordersResponse(31, 10), ordersResponse(6, 28));
        const { page, runQuery, scrollToNextPage } = openQueryPage("from Orders select distinct ShipVia, Employee");

        await runQuery();
        await scrollToNextPage(101);
        await scrollToNextPage(132);

        expect(sentStarts()).toEqual([0, 130, 171]);
        expect(page.totalResultsForUi()).toBe(138);
        expect(page.hasMoreUnboundedResults()).toBeFalse();
    });

    describe("going back to the Results tab", () => {
        it("requests the first page from Start 0 when the results had SkippedResults", async () => {
            mockServer(ordersResponse(89, 741), ordersResponse(89, 741));
            const { page, runQuery, goToResultsTab } = openQueryPage("from Orders select distinct Company");

            await runQuery();
            await goToResultsTab();

            expect(sentStarts()).toEqual([0, 0]);
            expect(page.totalResultsForUi()).toBe(89);
        });

        it("counts SkippedResults and loaded results from zero again", async () => {
            mockServer(ordersResponse(101, 29), ordersResponse(31, 10), ordersResponse(101, 29), ordersResponse(31, 10));
            const { page, runQuery, goToResultsTab, scrollToNextPage } = openQueryPage(
                "from Orders select distinct ShipVia, Employee"
            );

            await runQuery();
            await scrollToNextPage(101);
            await goToResultsTab();
            await scrollToNextPage(101);

            expect(sentStarts()).toEqual([0, 130, 0, 130]);
            expect(page.totalResultsForUi()).toBe(131);
            expect(page.hasMoreUnboundedResults()).toBeTrue();
        });

        it("ignores SkippedResults from a response that arrives after the results were reloaded", async () => {
            const lateResponse = mockPendingResponse();
            mockServer(ordersResponse(101, 29), ordersResponse(31, 10));
            const { runQuery, goToResultsTab, scrollToNextPage } = openQueryPage(
                "from Orders select distinct ShipVia, Employee"
            );

            const lateLoad = runQuery();
            await goToResultsTab();
            resolveWith(lateResponse, ordersResponse(101, 29));
            await lateLoad;
            await scrollToNextPage(101);

            expect(sentStarts()).toEqual([0, 0, 130]);
        });
    });
});
