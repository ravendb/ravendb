using System.Net;
using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Agents;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AgentTestQueryEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private sealed class Product
    {
        public string Name { get; set; } = "";
        public int Price { get; set; }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task TestQuery_runs_rql_with_parameters_against_the_app_database()
    {
        await using var app = await NewAppAsync();
        await SeedProductsAsync(app, 3);

        var response = await app.TestQueryAsync(new TestQueryRequest(
            "from Products where Price >= $minPrice order by Price",
            new Dictionary<string, JsonElement> { ["minPrice"] = JsonSerializer.SerializeToElement(20) }));

        Assert.Equal(2, response.TotalResults);
        Assert.False(response.IsTruncated);
        Assert.Equal(["Product 2", "Product 3"], response.Results.Select(r => r.GetProperty("Name").GetString()));
        Assert.Equal("Products", response.Results[0].GetProperty("@metadata").GetProperty("@collection").GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task TestQuery_honours_limit_in_the_query_text()
    {
        await using var app = await NewAppAsync();
        await SeedProductsAsync(app, 3);

        var response = await app.TestQueryAsync(new TestQueryRequest("from Products order by Price limit 1", null));

        Assert.Single(response.Results);
        Assert.False(response.IsTruncated);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task TestQuery_caps_results_and_reports_truncation()
    {
        await using var app = await NewAppAsync();
        await SeedProductsAsync(app, AgentQueryTester.MaxResults + 5);

        var response = await app.TestQueryAsync(new TestQueryRequest("from Products", null));

        Assert.Equal(AgentQueryTester.MaxResults, response.Results.Length);
        Assert.Equal(AgentQueryTester.MaxResults + 5, response.TotalResults);
        Assert.True(response.IsTruncated);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task TestQuery_returns_400_with_the_rql_error_for_an_invalid_query()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.TestQueryAsync(new TestQueryRequest("from Products where", null)));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains("Products", ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task TestQuery_returns_400_for_an_empty_query()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.TestQueryAsync(new TestQueryRequest("   ", null)));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task TestQuery_returns_404_for_unknown_app()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() =>
            Host.TestQueryAsync("no-such-app", new TestQueryRequest("from Products", null)));

        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    private static async Task SeedProductsAsync(QuillApp app, int count)
    {
        using var session = app.Store.OpenAsyncSession(app.Slug);
        for (var i = 1; i <= count; i++)
            await session.StoreAsync(new Product { Name = $"Product {i}", Price = i * 10 }, $"products/{i}");
        await session.SaveChangesAsync();
    }
}
