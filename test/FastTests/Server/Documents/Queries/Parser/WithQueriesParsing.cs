using System;
using System.IO;
using Newtonsoft.Json;
using Raven.Server.Documents.Queries.Parser;
using Xunit;

namespace FastTests.Server.Documents.Queries.Parser
{
    public class WithQueriesParsing : NoDisposalNeeded
    {
        [Theory]
        [InlineData("FROM Users AS u load u.Teams as Teams", "\"WITH\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"load\",\"Arguments\":[{\"Field\":\"u.Teams\"}]},\"Alias\":\"Teams\"}]")]
        [InlineData("FROM Users AS u include u.Teams", "\"WITH\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"include\",\"Arguments\":[{\"Field\":\"u.Teams\"}]}}]")]
        [InlineData(" from User as u load Employer as e select Name    as User, e.Name as employer   ", "\"WITH\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"doc\",\"Arguments\":[{\"Field\":\"Employer\"}]},\"Alias\":\"e\"}]")]
        public void CanParseFullQueries(string q, string json)
        {
            var parser = new QueryParser();
            parser.Init(q);

            var query = parser.Parse();
            var output = new StringWriter();
            //Query.WriteExpressionsList(new JsonTextWriter(output), query.With, "WITH", q);
            var actual = output.GetStringBuilder().ToString();

            Assert.Equal(json, actual);
        }
    }
}
