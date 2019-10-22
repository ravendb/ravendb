using Newtonsoft.Json;
using Raven.Server.Documents.Queries.Parser;
using System.IO;
using Raven.Server.Documents.Queries.AST;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Queries.Parser
{
    public class ComplexQueries : NoDisposalNeeded
    {
        public ComplexQueries(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(@"FROM Users (IsActive =false)", "{\"From\":\"Users\"}")]
        [InlineData(@"FROM Users (IsActive = true)
GROUP BY Country
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC", "{\"From\":\"Users\",\"GroupBy\":[{\"Expression\":\"Country\"}],\"Where\":{\"Between\":{\"Min\":21,\"Max\":30}},\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData(@"
FROM Users
WHERE boost(Age > 15, 2) 
ORDER BY LastName
SELECT sum(Age), Name as Username
", "{\"From\":\"Users\",\"Where\":{\"Method\":\"boost\",\"Arguments\":[{\"Type\":\"GreaterThan\",\"Left\":\"Age\",\"Right\":15},2]},\"OrderBy\":[{\"Field\":\"LastName\",\"Ascending\":true}],\"Select\":[{\"Expression\":{\"Method\":\"sum\",\"Arguments\":[\"Age\"]}},{\"Expression\":\"Name\",\"Alias\":\"Username\"}]}")]
        [InlineData(@"FROM Users
ORDER BY Age AS double DESC, Name ASC", "{\"From\":\"Users\",\"OrderBy\":[{\"Field\":\"Age\",\"FieldType\":\"Double\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData(@"FROM Users GROUP BY Country WHERE sum(Weight) > 100 SELECT sum(Weight) ", "{\"From\":\"Users\",\"GroupBy\":[{\"Expression\":\"Country\"}],\"Where\":{\"Type\":\"GreaterThan\",\"Left\":{\"Method\":\"sum\",\"Arguments\":[\"Weight\"]},\"Right\":100},\"Select\":[{\"Expression\":{\"Method\":\"sum\",\"Arguments\":[\"Weight\"]}}]}")]
        [InlineData(@"FROM Posts WHERE Tags[].Name = 'Any'", "{\"From\":\"Posts\",\"Where\":{\"Type\":\"Equal\",\"Left\":\"Tags[].Name\",\"Right\":\"Any\"}}")]
        [InlineData(@"FROM Users AS u", "{\"From\":\"Users\",\"Alias\":\"u\"}")]
        [InlineData(@"FROM Users WHERE search(Name, 'oren')", "{\"From\":\"Users\",\"Where\":{\"Method\":\"search\",\"Arguments\":[\"Name\",\"oren\"]}}")]
        [InlineData(@"FROM Users (IsActive = null)
GROUP BY Country
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC", "{\"From\":\"Users\",\"GroupBy\":[{\"Expression\":\"Country\"}],\"Where\":{\"Between\":{\"Min\":21,\"Max\":30}},\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData(@"FROM Users (IsActive = true)
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC", "{\"From\":\"Users\",\"Where\":{\"Between\":{\"Min\":21,\"Max\":30}},\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData(@"FROM Users (IsActive = true) SELECT Age ", "{\"From\":\"Users\",\"Select\":[{\"Expression\":\"Age\"}]}")]
        [InlineData(@"FROM Users GROUP BY Country WHERE count() > 100 SELECT count()", "{\"From\":\"Users\",\"GroupBy\":[{\"Expression\":\"Country\"}],\"Where\":{\"Type\":\"GreaterThan\",\"Left\":{\"Method\":\"count\",\"Arguments\":[]},\"Right\":100},\"Select\":[{\"Expression\":{\"Method\":\"count\",\"Arguments\":[]}}]}")]
        [InlineData(@"FROM Users", "{\"From\":\"Users\"}")]
        [InlineData(@"FROM Users GROUP BY array(Tags[])", "{\"From\":\"Users\",\"GroupBy\":[{\"Expression\":{\"Method\":\"array\",\"Arguments\":[\"Tags[]\"]}}]}")]
        [InlineData(@"from Orders
group by Month(OrderedAt) as month, Year(OrderedAt) as year
where sum(Total) >= 10000
select sum(Total) as Total", "{\"From\":\"Orders\",\"GroupBy\":[{\"Expression\":{\"Method\":\"Month\",\"Arguments\":[\"OrderedAt\"]},\"Alias\":\"month\"},{\"Expression\":{\"Method\":\"Year\",\"Arguments\":[\"OrderedAt\"]},\"Alias\":\"year\"}],\"Where\":{\"Type\":\"GreaterThanEqual\",\"Left\":{\"Method\":\"sum\",\"Arguments\":[\"Total\"]},\"Right\":10000},\"Select\":[{\"Expression\":{\"Method\":\"sum\",\"Arguments\":[\"Total\"]},\"Alias\":\"Total\"}]}")]
        [InlineData(@"FROM Users GROUP BY 1 SELECT count()", "{\"From\":\"Users\",\"GroupBy\":[{\"Expression\":1}],\"Select\":[{\"Expression\":{\"Method\":\"count\",\"Arguments\":[]}}]}")]
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
            //File.AppendAllText("out.txt", $"[InlineData(@\"{q}\", \"{actual.Replace("\"", "\\\"")}\")]\r\n");
            Assert.Equal(json, actual);
        }
    }
}
