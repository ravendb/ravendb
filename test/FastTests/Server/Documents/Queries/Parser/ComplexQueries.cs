using Newtonsoft.Json;
using Raven.Server.Documents.Queries.Parser;
using System.IO;
using Xunit;

namespace FastTests.Server.Documents.Queries.Parser
{
    public class ComplexQueries : NoDisposalNeeded
    {
        [Theory]
        [InlineData("FROM Users", "{\"From\":{\"Index\":false,\"Source\":\"Users\"}}")]
        [InlineData("FROM Users AS u", "{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Alias\":\"u\"}}")]
        [InlineData("FROM Users WHERE search(Name, 'oren')", "{\"From\":{\"Index\":false,\"Source\":\"Users\"},\"Where\":{\"Type\":\"Method\",\"Method\":\"search\",\"Arguments\":[{\"Field\":\"Name\"},\"oren'\"]}}")]
        [InlineData(@"FROM Users (IsActive = null)
GROUP BY Country
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC", "{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":null}},\"GroupBy\":[\"Country\"],\"Where\":{\"Type\":\"Between\",\"Field\":\"Age\",\"Min\":21,\"Max\":30},\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData(@"FROM Users (IsActive = true)
GROUP BY Country
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC", "{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":true}},\"GroupBy\":[\"Country\"],\"Where\":{\"Type\":\"Between\",\"Field\":\"Age\",\"Min\":21,\"Max\":30},\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData("FROM Users (IsActive =false)", "{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":false}}}")]
        [InlineData(@"FROM Users (IsActive = true) SELECT Age ", "{\"Select\":[{\"Expression\":\"Age\"}],\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":true}}}")]
        [InlineData(@"FROM Users (IsActive = true)
WHERE Age BETWEEN 21 AND 30", "{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":true}},\"Where\":{\"Type\":\"Between\",\"Field\":\"Age\",\"Min\":21,\"Max\":30}}")]
        [InlineData(@"FROM Users (IsActive = true)
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC", "{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":true}},\"Where\":{\"Type\":\"Between\",\"Field\":\"Age\",\"Min\":21,\"Max\":30},\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData(@"
FROM Users
WHERE boost(Age > 15, 2) 
ORDER BY LastName
SELECT sum(Age), Name as Username
", "{\"Select\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"sum\",\"Arguments\":[{\"Field\":\"Age\"}]}},{\"Expression\":\"Name\",\"Alias\":\"Username\"}],\"From\":{\"Index\":false,\"Source\":\"Users\"},\"Where\":{\"Type\":\"Method\",\"Method\":\"boost\",\"Arguments\":[{\"Type\":\"GreaterThan\",\"Field\":\"Age\",\"Value\":\"15\"},2]},\"OrderBy\":[{\"Field\":\"LastName\",\"Ascending\":true}]}")]
        [InlineData(@"FROM Users
ORDER BY Age AS double DESC, Name ASC", "{\"From\":{\"Index\":false,\"Source\":\"Users\"},\"OrderBy\":[{\"Field\":\"Age\",\"FieldType\":\"Double\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData("FROM Posts WHERE Tags[].Name = 'Any'", "{\"From\":{\"Index\":false,\"Source\":\"Posts\"},\"Where\":{\"Type\":\"Equal\",\"Field\":\"Tags[].Name\",\"Value\":\"Any'\"}}")]
        [InlineData("FROM Users GROUP BY Country WHERE count() > 100 SELECT count()", "{\"Select\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"count\",\"Arguments\":[]}}],\"From\":{\"Index\":false,\"Source\":\"Users\"},\"GroupBy\":[\"Country\"],\"Where\":{\"Type\":\"Method\",\"Method\":\"count\",\"Arguments\":[{\"Type\":\"GreaterThan\",\"Field\":null,\"Value\":\"100\"}]}}")]
        [InlineData("FROM Users GROUP BY Country WHERE sum(Weight) > 100 SELECT sum(Weight) ", "{\"Select\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"sum\",\"Arguments\":[{\"Field\":\"Weight\"}]}}],\"From\":{\"Index\":false,\"Source\":\"Users\"},\"GroupBy\":[\"Country\"],\"Where\":{\"Type\":\"Method\",\"Method\":\"sum\",\"Arguments\":[{\"Field\":\"Weight\"},{\"Type\":\"GreaterThan\",\"Field\":null,\"Value\":\"100\"}]}}")]

        public void CanParseFullQueries(string q, string json)
        {
            var parser = new QueryParser();
            parser.Init(q);

            var query = parser.Parse();
            var output = new StringWriter();
            query.ToJsonAst(new JsonTextWriter(output));
            var actual = output.GetStringBuilder().ToString();
            //Console.WriteLine(actual.Replace("\"", "\\\""));
            Assert.Equal(json, actual);
        }
    }
}
