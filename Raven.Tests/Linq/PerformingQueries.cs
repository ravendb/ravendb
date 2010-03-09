using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database.Json;
using Raven.Database.Linq;
using Xunit;

namespace Raven.Tests.Linq
{
    public class PerformingQueries
    {
        private const string query =
            @"
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = doc.size };
";

        [Fact]
        public void Can_query_json()
        {
            var documents =
                GetDocumentsFromString(
                    @"[
{'type':'page', title: 'hello', content: 'foobar', size: 2, '@metadata': {'@id': 1}},
{'type':'page', title: 'there', content: 'foobar 2', size: 3, '@metadata': {'@id': 2} },
{'type':'revision', size: 4, _id: 3}
]");
            var transformer = new DynamicViewCompiler("pagesByTitle", query, null);
            var compiledQuery = transformer.GenerateInstance();
            var actual = compiledQuery.MapDefinition(documents)
                .Cast<object>().ToArray();
            var expected = new[]
            {
                "{ Key = hello, Value = foobar, Size = 2, __document_id = 1 }",
                "{ Key = there, Value = foobar 2, Size = 3, __document_id = 2 }"
            };

            Assert.Equal(expected.Length, actual.Length);
            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], actual[i].ToString());
            }
        }

        public static IEnumerable<dynamic> GetDocumentsFromString(string json)
        {
            return JArray.Parse(json).Select(JsonToExpando.Convert);
        }
    }
}