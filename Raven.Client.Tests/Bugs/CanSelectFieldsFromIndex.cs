using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
    public class CanSelectFieldsFromIndex : BaseClientTest
    {
        [Fact]
        public void SelectFieldsFromIndex()
        {
            using(var store = NewDocumentStore())
            {
                store.DocumentDatabase.Put("ayende", null,
                                           JObject.Parse(
                                               @"{
	'name': 'ayende',
	'email': 'ayende@ayende.com',
	'projects': [
		'rhino mocks',
		'nhibernate',
		'rhino service bus',
		'rhino divan db',
		'rhino persistent hash table',
		'rhino distributed hash table',
		'rhino etl',
		'rhino security',
		'rampaging rhinos'
	]
}"),
                                           new JObject(), null);

                store.DatabaseCommands.PutIndex("EmailAndProject",
                                                new IndexDefinition
                                                {
                                                    Map =
                                                    "from doc in docs from project in doc.projects select new {doc.email, doc.name, project };",
                                                    Stores =
                                                    {
                                                        {"email", FieldStorage.Yes},
                                                        {"name", FieldStorage.Yes},
                                                        {"project", FieldStorage.Yes}
                                                    }
                                                });


                while (store.DatabaseCommands.Query("EmailAndProject", new IndexQuery(), null).IsStale)
                    Thread.Sleep(100);

                var queryResult = store.DatabaseCommands.Query("EmailAndProject", new IndexQuery
                {
                    FieldsToFetch = new string[] {"email"}
                }, null);

                Assert.Equal(9, queryResult.Results.Count);

                foreach (var result in queryResult.Results)
                {
                    Assert.Equal("ayende@ayende.com", result.Value<string>("email"));
                }
            }
        }
    }
}