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
        [InlineData("FROM Users AS u WITH load(u.Teams) as Teams", "\"WITH\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"load\",\"Arguments\":[{\"Field\":\"u.Teams\"}]},\"Alias\":\"Teams\"}]")]
        [InlineData("FROM Users AS u WITH include(u.Teams)", "\"WITH\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"include\",\"Arguments\":[{\"Field\":\"u.Teams\"}]}}]")]
        [InlineData("select Name    as User, e.Name as employer   from User as u with doc(Employer) as e", "\"WITH\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"doc\",\"Arguments\":[{\"Field\":\"Employer\"}]},\"Alias\":\"e\"}]")]
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
