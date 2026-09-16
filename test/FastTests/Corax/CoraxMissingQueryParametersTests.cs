using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Linq;
using Raven.Client.Http;
using Raven.Server.Extensions;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class CoraxMissingQueryParametersTests : RavenTestBase
{
    public CoraxMissingQueryParametersTests(ITestOutputHelper output) : base(output)
    {
    }

    private sealed class Movie
    {
        public string Id { get; set; }
        public string Genres { get; set; }
        public DateTime ReleaseDate { get; set; }
    }

    // A GET query that references $params but supplies them as raw query-string keys (the way the
    // Studio "query" URL is sometimes hand-edited) rather than a `parameters` JSON blob leaves
    // IndexQueryServerSide.QueryParameters null. The Corax plan builder used to dereference that null
    // blittable in ResolveBindingScalar and throw a NullReferenceException; it must instead surface the
    // standard InvalidQueryException ("the actual values of parameters were not provided").
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task ParameterizedGetQueryWithoutParametersThrowsInvalidQueryInsteadOfNre(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Movie { Genres = "romance", ReleaseDate = new DateTime(2011, 1, 1) });
            await session.SaveChangesAsync();
        }

        // Build and populate the auto-index up front, so the raw GET below reaches the plan builder.
        using (var session = store.OpenAsyncSession())
        {
            await session.Advanced
                .AsyncRawQuery<Movie>("from Movies where Genres = $genre and ReleaseDate >= $date order by ReleaseDate as string desc")
                .AddParameter("genre", "romance")
                .AddParameter("date", "2010")
                .WaitForNonStaleResults()
                .ToListAsync();
        }

        const string rql = "from Movies where Genres = $genre and ReleaseDate >= $date order by ReleaseDate as string desc";
        var url = $"{store.Urls[0]}/databases/{store.Database}/queries?query={Uri.EscapeDataString(rql)}&date=2010&genre=romance";

        var requestExecutor = store.GetRequestExecutor();
        var message = new HttpRequestMessage(HttpMethod.Get, url).WithConventions(requestExecutor.Conventions);
        var response = await requestExecutor.HttpClient.SendAsync(message);
        var body = await response.Content.ReadAsStringAsync();

        Assert.DoesNotContain(nameof(NullReferenceException), body);
        Assert.Contains("were not provided", body);
    }
}
