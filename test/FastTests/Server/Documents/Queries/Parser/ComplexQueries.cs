using Newtonsoft.Json;
using Raven.Server.Documents.Queries.Parser;
using System.IO;
using Raven.Server.Documents.Queries.AST;
using Xunit;

namespace FastTests.Server.Documents.Queries.Parser
{
    public class ComplexQueries : NoDisposalNeeded
    {
        [Theory]
        [InlineData("FROM Users", "{\"From\":\"Users\"}")]
        [InlineData("FROM Users (IsActive =false)", "{\"From\":\"Users\"}")]
        [InlineData("FROM Users AS u", "{\"From\":\"Users\",\"Alias\":\"u\"}")]
        [InlineData("FROM Users (IsActive = true) SELECT Age ", "{\"From\":\"Users\",\"Select\":[{\"Expression\":\"Age\"}]}")]
        [InlineData(@"FROM Users (IsActive = true)
GROUP BY Country
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC", "{\"From\":\"Users\",\"GroupBy\":[\"Country\"],\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData("FROM Posts WHERE Tags[].Name = 'Any'", "{\"From\":\"Posts\",\"Where\":{\"Type\":\"Equal\",\"Left\":\"Tags[].Name\",\"Right\":\"Any\"}}")]
        [InlineData("FROM Users GROUP BY Country WHERE sum(Weight) > 100 SELECT sum(Weight) ", "{\"From\":\"Users\",\"GroupBy\":[\"Country\"],\"Select\":[{\"Expression\":{\"Method\":\"sum\",\"Arguments\":[\"Weight\"]}}]}")]
        [InlineData(@"FROM Users
ORDER BY Age AS double DESC, Name ASC", "{\"From\":\"Users\",\"OrderBy\":[{\"Field\":\"Age\",\"FieldType\":\"Double\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData(@"
FROM Users
WHERE boost(Age > 15, 2) 
ORDER BY LastName
SELECT sum(Age), Name as Username
", "{\"From\":\"Users\",\"OrderBy\":[{\"Field\":\"LastName\",\"Ascending\":true}],\"Select\":[{\"Expression\":{\"Method\":\"sum\",\"Arguments\":[\"Age\"]}},{\"Expression\":\"Name\",\"Alias\":\"Username\"}]}")]
        [InlineData(@"FROM Users (IsActive = null)
GROUP BY Country
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC", "{\"From\":\"Users\",\"GroupBy\":[\"Country\"],\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData("FROM Users GROUP BY Country WHERE count() > 100 SELECT count()", "{\"From\":\"Users\",\"GroupBy\":[\"Country\"],\"Select\":[{\"Expression\":{\"Method\":\"count\",\"Arguments\":[]}}]}")]
        [InlineData(@"FROM Users (IsActive = true)
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC", "{\"From\":\"Users\",\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData("FROM Users WHERE search(Name, 'oren')", "{\"From\":\"Users\"}")]
        public void CanParseFullQueries(string q, string json)
        {
            var parser = new QueryParser();
            parser.Init(q);

            var query = parser.Parse();
            var output = new StringWriter();
            var jsonTextWriter = new JsonTextWriter(output);
            jsonTextWriter.WriteStartObject();
            new JsonQueryVisitor(jsonTextWriter).Visit(query);
            jsonTextWriter.WriteEndObject();
            var actual = output.GetStringBuilder().ToString();
            //File.AppendAllText("out.txt", $"[InlineData(\"{q}\", \"{actual.Replace("\"", "\\\"")}\")]\r\n");
            Assert.Equal(json, actual);
        }
    }
}
