using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Session.Tokens;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16963 : RavenTestBase
    {
        public RavenDB_16963(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task can_use_keywords_in_include()
        {
            using (var store = GetDocumentStore())
            {
                var referencedDocs = new List<string>();

                using (var session = store.OpenAsyncSession())
                {
                    var expando = new ExpandoObject();
                    var dict = (IDictionary<string, object>)expando;

                    foreach (var keyword in QueryToken.RqlKeywords)
                    {
                        var referenced = new Referenced
                        {
                            Name = keyword
                        };
                        await session.StoreAsync(referenced, keyword);

                        dict[keyword] = referenced.Id;
                        referencedDocs.Add(referenced.Id);
                    }

                    var metadata = new ExpandoObject();
                    ((IDictionary<string, object>)metadata)[Constants.Documents.Metadata.Collection] = "Users";

                    dict[Constants.Documents.Metadata.Key] = metadata;
                    await session.StoreAsync(expando);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults());

                    var expected = "from 'Users' include ";
                    var first = true;
                    foreach (var keyword in QueryToken.RqlKeywords)
                    {
                        query = query.Include(keyword);

                        if (first == false)
                            expected += ",";

                        first = false;
                        expected += $"'{keyword}'";
                    }

                    var queryString = query.ToString();
                    Assert.Equal(expected, queryString);

                    var result = await query.ToListAsync();
                    Assert.Equal(1, result.Count);

                    foreach (var docId in referencedDocs)
                    {
                        var doc = await session.LoadAsync<Referenced>(docId);
                        Assert.NotNull(doc);
                        Assert.Equal(doc.Id, doc.Name);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        private class User
        {
        }

        private class Referenced
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }
    }
}
