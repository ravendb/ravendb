using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents;

// A multi_get sub-request takes only the string values of its Headers object as headers, and ignores anything else
public class MultiGetHeaders(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData("\"a string\"")]
    [InlineData("12345")]
    [InlineData("{\"nested\":1}")]
    [InlineData("[1,2,3]")]
    public async Task AWrongTypedHeaderValueMustNotProduceAServerError(string headerValueJson)
    {
        using var store = GetDocumentStore();

        // the /databases/<name> prefix matters - without it the sub-request never routes, and the header code never runs
        var body = $$"""
            {
              "Requests": [
                {
                  "Url": "/databases/{{store.Database}}/docs",
                  "Query": "?id=users/1",
                  "Method": "GET",
                  "Headers": { "X-Probe": {{headerValueJson}} }
                }
              ]
            }
            """;

        using var client = new HttpClient();
        var url = $"{store.Urls[0]}/databases/{store.Database}/multi_get";
        using var response = await client.PostAsync(url, new StringContent(body, Encoding.UTF8, "application/json"));
        var content = await response.Content.ReadAsStringAsync();

        Assert.DoesNotContain("There is no handler for path", content);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.False(content.Contains("FormatException") || content.Contains("\"StatusCode\":500"), content);
    }
}
