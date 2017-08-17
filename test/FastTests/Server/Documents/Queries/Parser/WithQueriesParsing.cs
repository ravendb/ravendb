using System.IO;
using Newtonsoft.Json;
using Raven.Server.Documents.Queries.Parser;
using Xunit;

namespace FastTests.Server.Documents.Queries.Parser
{
    public class WithQueriesParsing : NoDisposalNeeded
    {
        [Theory]
        [InlineData("FROM Users AS u WITH load(u.Teams) as Teams", "\"WITH\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"load\",\"Arguments\":[{\"Field\":\"u.Teams\"}]},\"Alias\":\"Teams\"}]")]
        [InlineData("FROM Users AS u WITH include(u.Teams)", "\"WITH\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"include\",\"Arguments\":[{\"Field\":\"u.Teams\"}]}]")]
        public void CanParseFullQueries(string q, string json)
        {
            var parser = new QueryParser();
            parser.Init(q);

            var query = parser.Parse();
            var output = new StringWriter();
            Query.WriteSelectOrWith(new JsonTextWriter(output), query.With, "WITH", q);
            var actual = output.GetStringBuilder().ToString();
            Assert.Equal(json, actual);
        }
    }
}
