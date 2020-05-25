using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Queries;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Server.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13563 : RavenTestBase
    {
        public RavenDB_13563(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldGenerateSimpleLuceneBooleanQueriesWithoutManyBrackets()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var serializer = (JsonSerializer)store.Conventions.Serialization.CreateSerializer();

                    using (var context = JsonOperationContext.ShortTermSingleUse())
                    {
                        var usersOr = session.Advanced.DocumentQuery<User>()
                            .WhereEquals(x => x.Name, "John").Boost(2)
                            .OrElse()
                            .WhereEquals(x => x.Name, "Bob").Boost(3)
                            .OrElse()
                            .WhereEquals(x => x.Name, "George").Boost(4)
                            .OrElse()
                            .WhereEquals(x => x.Name, "Georgina").Boost(5)
                            .OrElse()
                            .WhereEquals(x => x.Name, "Jane").Boost(6)
                            .GetIndexQuery();

                        Assert.Equal("Name:john^2.0 Name:bob^3.0 Name:george^4.0 Name:georgina^5.0 Name:jane^6.0", GetLuceneQuery(usersOr, context, serializer));

                        var usersAnd = session.Advanced.DocumentQuery<User>()
                            .WhereEquals(x => x.Name, "John").Boost(2)
                            .AndAlso()
                            .WhereEquals(x => x.Name, "Bob").Boost(3)
                            .AndAlso()
                            .WhereEquals(x => x.Name, "George").Boost(4)
                            .AndAlso()
                            .WhereEquals(x => x.Name, "Georgina").Boost(5)
                            .AndAlso()
                            .WhereEquals(x => x.Name, "Jane").Boost(6)
                            .GetIndexQuery();

                        Assert.Equal("+Name:john^2.0 +Name:bob^3.0 +Name:george^4.0 +Name:georgina^5.0 +Name:jane^6.0", GetLuceneQuery(usersAnd, context, serializer));

                        var usersAndOrMixed = session.Advanced.DocumentQuery<User>()
                            .WhereEquals(x => x.Name, "John").Boost(2)
                            .AndAlso()
                            .WhereEquals(x => x.Name, "Bob").Boost(3)
                            .AndAlso()
                            .WhereEquals(x => x.Name, "George").Boost(4)
                            .OrElse()
                            .WhereEquals(x => x.Name, "Georgina").Boost(5)
                            .OrElse()
                            .WhereEquals(x => x.Name, "Jane").Boost(6)
                            .GetIndexQuery();

                        Assert.Equal("(+Name:john^2.0 +Name:bob^3.0 +Name:george^4.0) Name:georgina^5.0 Name:jane^6.0", GetLuceneQuery(usersAndOrMixed, context, serializer));

                        var usersAndOrMixed2 = session.Advanced.DocumentQuery<User>()
                            .WhereEquals(x => x.Name, "John").Boost(2)
                            .OrElse()
                            .WhereEquals(x => x.Name, "Bob").Boost(3)
                            .OrElse()
                            .WhereEquals(x => x.Name, "George").Boost(4)
                            .AndAlso()
                            .WhereEquals(x => x.Name, "Georgina").Boost(5)
                            .AndAlso()
                            .WhereEquals(x => x.Name, "Jane").Boost(6)
                            .GetIndexQuery();

                        Assert.Equal("Name:john^2.0 Name:bob^3.0 (+Name:george^4.0 +Name:georgina^5.0 +Name:jane^6.0)", GetLuceneQuery(usersAndOrMixed2, context, serializer));

                        var usersAndOrMixed3 = session.Advanced.DocumentQuery<User>()
                            .WhereEquals(x => x.Name, "John").Boost(2)
                            .OrElse()
                            .WhereEquals(x => x.Name, "Bob").Boost(3)
                            .AndAlso()
                            .WhereEquals(x => x.Name, "George").Boost(4)
                            .OrElse()
                            .WhereEquals(x => x.Name, "Georgina").Boost(5)
                            .AndAlso()
                            .WhereEquals(x => x.Name, "Jane").Boost(6)
                            .GetIndexQuery();

                        Assert.Equal("Name:john^2.0 (+Name:bob^3.0 +Name:george^4.0) (+Name:georgina^5.0 +Name:jane^6.0)", GetLuceneQuery(usersAndOrMixed3, context, serializer));
                    }

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

            return GetLuceneQuery(indexQuery, context, jsonSerializer);
        }

        private static string GetLuceneQuery(IndexQuery indexQuery, JsonOperationContext context, JsonSerializer jsonSerializer)
        {
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
