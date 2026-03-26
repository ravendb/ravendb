using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_24435 : RavenTestBase
    {
        public RavenDB_24435(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task GetDatabaseRecord_Should_Return_URL_Encoded_DatabaseName_In_Header_When_Database_Is_Missing()
        {
            var databaseName = $"database-møøse-{Guid.NewGuid()}";

            using var httpClient = new HttpClient();
            var requestUri = $"{Server.WebUrl}/admin/databases?name={Uri.EscapeDataString(databaseName)}";

            await AssertDatabaseMissingHeaderValueAsync(httpClient, requestUri, databaseName);
            
            requestUri = $"{Server.WebUrl}/topology?name={Uri.EscapeDataString(databaseName)}";
            
            await AssertDatabaseMissingHeaderValueAsync(httpClient, requestUri, databaseName);
        }
        
        private async Task AssertDatabaseMissingHeaderValueAsync(HttpClient httpClient, string requestUri, string expectedDatabaseName)
        {
            var response = await httpClient.GetAsync(requestUri);
            
            Assert.True(response.Headers.Contains(Constants.Headers.DatabaseMissing));
            var headerValues = response.Headers.GetValues(Constants.Headers.DatabaseMissing);
            var headerValue = Assert.Single(headerValues);

            var decodedName = Uri.UnescapeDataString(headerValue);
            Assert.Equal(expectedDatabaseName, decodedName);
        }
    }
}
