using Newtonsoft.Json;
using Raven.Server.Documents.Queries.Parser;
using System.IO;
using Xunit;

namespace FastTests.Server.Documents.Queries.Parser
{
    public class ComplexQueries
    {
        [Theory]
        [InlineData("FROM Users", "{\"From\":{\"Index\":false,\"Source\":\"Users\"}}")]
        [InlineData("select Name    as User, e.Name as employer  with doc(Employer) as e from User as u", "{\"Select\":[{\"Expression\":\"Name\",\"Alias\":\"User\"},{\"Expression\":\"e.Name\",\"Alias\":\"employer\"}],\"With\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"doc\",\"Arguments\":[{\"Field\":\"Employer\"}]},\"Alias\":\"e\"}],\"From\":{\"Index\":false,\"Source\":\"User\",\"Alias\":\"u\"}}")]
        [InlineData("FROM Users AS u", "{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Alias\":\"u\"}}")]
        [InlineData("FROM Users WHERE search(Name, 'oren')", "{\"From\":{\"Index\":false,\"Source\":\"Users\"},\"Where\":{\"Type\":\"Method\",\"Method\":\"search\",\"Arguments\":[{\"Field\":\"Name\"},\"oren'\"]}}")]
        [InlineData(@"FROM (Users, IsActive = null)
GROUP BY Country
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC","{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":null}},\"GroupBy\":[\"Country\"],\"Where\":{\"Type\":\"Between\",\"Field\":\"Age\",\"Min\":21,\"Max\":30},\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData(@"FROM (Users, IsActive = true)
GROUP BY Country
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC","{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":true}},\"GroupBy\":[\"Country\"],\"Where\":{\"Type\":\"Between\",\"Field\":\"Age\",\"Min\":21,\"Max\":30},\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData("FROM (Users, IsActive =false)", "{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":false}}}")]
        [InlineData(@"SELECT Age FROM (Users, IsActive = true)","{\"Select\":[{\"Expression\":\"Age\"}],\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":true}}}")]
        [InlineData(@"FROM (Users, IsActive = true)
WHERE Age BETWEEN 21 AND 30", "{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":true}},\"Where\":{\"Type\":\"Between\",\"Field\":\"Age\",\"Min\":21,\"Max\":30}}")]
        [InlineData(@"FROM (Users, IsActive = true)
WHERE Age BETWEEN 21 AND 30
ORDER BY Age DESC, Name ASC", "{\"From\":{\"Index\":false,\"Source\":\"Users\",\"Filter\":{\"Type\":\"Equal\",\"Field\":\"IsActive\",\"Value\":true}},\"Where\":{\"Type\":\"Between\",\"Field\":\"Age\",\"Min\":21,\"Max\":30},\"OrderBy\":[{\"Field\":\"Age\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]
        [InlineData(@"
SELECT sum(Age), Name as Username
FROM Users
WHERE boost(Age > 15, 2) 
ORDER BY LastName
", "{\"Select\":[{\"Expression\":{\"Type\":\"Method\",\"Method\":\"sum\",\"Arguments\":[{\"Field\":\"Age\"}]}},{\"Expression\":\"Name\",\"Alias\":\"Username\"}],\"From\":{\"Index\":false,\"Source\":\"Users\"},\"Where\":{\"Type\":\"Method\",\"Method\":\"boost\",\"Arguments\":[{\"Type\":\"GreaterThen\",\"Field\":\"Age\",\"Value\":\"15\"},2]},\"OrderBy\":[{\"Field\":\"LastName\",\"Ascending\":true}]}")]
        [InlineData(@"FROM Users
ORDER BY Age AS double DESC, Name ASC", "{\"From\":{\"Index\":false,\"Source\":\"Users\"},\"OrderBy\":[{\"Field\":\"Age\",\"FieldType\":\"Double\",\"Ascending\":false},{\"Field\":\"Name\",\"Ascending\":true}]}")]

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