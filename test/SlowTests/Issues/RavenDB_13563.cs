using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Json;
using Raven.Server.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13563 : RavenTestBase
    {
        [Fact]
        public void ShouldGenerateSimpleLuceneBooleanQueriesWithoutManyBrackets()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var serializer = store.Conventions.CreateSerializer();

                    using (var context = JsonOperationContext.ShortTermSingleUse())
                    {
                        var usersOr = session.Query<User>()
                            .Customize(x => x.NoCaching())
                            .Where(x => x.Name == "John" || x.Name == "Bob" || x.Name == "George" || x.Name == "Georgina" || x.Name == "Jane");
                        Assert.Equal("Name:john Name:bob Name:george Name:georgina Name:jane", GetLuceneQuery(usersOr, context, serializer));

                        var usersAnd = session.Query<User>()
                            .Customize(x => x.NoCaching())
                            .Where(x => x.Name == "John" && x.Name == "Bob" && x.Name == "George" && x.Name == "Georgina" && x.Name == "Jane");

                        Assert.Equal("+Name:john +Name:bob +Name:george +Name:georgina +Name:jane", GetLuceneQuery(usersAnd, context, serializer));

                        var usersAndOrMixed = session.Query<User>()
                            .Customize(x => x.NoCaching())
                            .Where(x => x.Name == "John" && x.Name == "Bob" && x.Name == "George" || x.Name == "Georgina" || x.Name == "Jane");

                        Assert.Equal("(+Name:john +Name:bob +Name:george) Name:georgina Name:jane", GetLuceneQuery(usersAndOrMixed, context, serializer));

                        var usersOrAndMixed2 = session.Query<User>()
                            .Customize(x => x.NoCaching())
                            .Where(x => x.Name == "John" || x.Name == "Bob" || x.Name == "George" && x.Name == "Georgina" && x.Name == "Jane");

                        Assert.Equal("Name:john Name:bob (+Name:george +Name:georgina +Name:jane)", GetLuceneQuery(usersOrAndMixed2, context, serializer));

                        var usersOrAndMixed3 = session.Query<User>()
                            .Customize(x => x.NoCaching())
                            .Where(x => x.Name == "John" || x.Name == "Bob" && x.Name == "George" || x.Name == "Georgina" && x.Name == "Jane");

                        Assert.Equal("Name:john (+Name:bob +Name:george) (+Name:georgina +Name:jane)", GetLuceneQuery(usersOrAndMixed3, context, serializer));
                    }
                }
            }
        }

        private static string GetLuceneQuery(IQueryable<User> query, JsonOperationContext context, JsonSerializer jsonSerializer)
        {
            var indexQuery = RavenTestHelper.GetIndexQuery(query);

            using (var writer = new BlittableJsonWriter(context))
            {
                jsonSerializer.Serialize(writer, indexQuery.QueryParameters);

                writer.FinalizeDocument();

                using (var blittableParameters = writer.CreateReader())
                {
                    var indexQueryServerSide = new IndexQueryServerSide(indexQuery.Query, blittableParameters);

                    var luceneQuery = QueryBuilder.BuildQuery(null,
                        null,
                        indexQueryServerSide.Metadata,
                        indexQueryServerSide.Metadata.Query.Where,
                        null,
                        blittableParameters, null,
                        null);

                    return luceneQuery.ToString();
                }
            }
        }
    }
}
