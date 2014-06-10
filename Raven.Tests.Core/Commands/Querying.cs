using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.Commands
{
    public class Querying : RavenCoreTestBase
    {
        [Fact]
        public void CanDoSimpleQueryOnDatabase()
        {
            const string indexName = "CompaniesByName";
            using (var store = GetDocumentStore())
            {
                var contact1 = new Contact { FirstName = "Expression Name" };
                var contact2 = new Contact { FirstName = "Expression First Name" };
                var contact3 = new Contact { FirstName = "First Name" };

                store.DatabaseCommands.Put("contacts/1", null, RavenJObject.FromObject(contact1), new RavenJObject { { "Raven-Entity-Name", "Contacts" } });
                store.DatabaseCommands.Put("contacts/2", null, RavenJObject.FromObject(contact2), new RavenJObject { { "Raven-Entity-Name", "Contacts" } });
                store.DatabaseCommands.Put("contacts/3", null, RavenJObject.FromObject(contact3), new RavenJObject { { "Raven-Entity-Name", "Contacts" } });

                store.DatabaseCommands.PutIndex(indexName, new IndexDefinition()
				{
					Map = "from contact in docs.Contacts select new { contact.FirstName }"
				}, false);
                WaitForIndexing(store);

                var companies = store.DatabaseCommands.Query(indexName, new IndexQuery { Query = "" }, null);
                Assert.Equal(3, companies.TotalResults);
                Assert.Equal("Expression Name", companies.Results[0].Value<string>("FirstName"));
                Assert.Equal("Expression First Name", companies.Results[1].Value<string>("FirstName"));
                Assert.Equal("First Name", companies.Results[2].Value<string>("FirstName"));
            }
        }

        [Fact]
        public void CanStreamQueryResult()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_ByName();
                index.Execute(store);

                for (int i = 0; i < 30; i++)
                {
                    store.DatabaseCommands.Put("users/" + i, null, RavenJObject.FromObject(new User { Name = "Name"+i }), new RavenJObject { { "Raven-Entity-Name", "Users" } });
                }
                WaitForIndexing(store);

                int count = 0;
                QueryHeaderInformation queryHeaders = null;
                var reader = store.DatabaseCommands.StreamQuery(index.IndexName, new IndexQuery { Query = "" }, out queryHeaders);
                while (reader.MoveNext())
                {
                    Assert.Equal("Name" + count, reader.Current.Value<string>("Name"));
                    count++;
                }
                Assert.Equal(30, count);
            }
        }
    }
}
