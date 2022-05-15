using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13349 : RavenTestBase
    {
        public RavenDB_13349(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            [JsonProperty(PropertyName = "first_name")]
            public string Name { get; set; }
            [JsonProperty(PropertyName = "last_name")]
            public string LastName { get; set; }
            [JsonProperty(PropertyName = "user_address")]
            public Address Address { get; set; }
            public int Count { get; set; }
            public int Age { get; set; }
        }

        private class Address
        {
            public string Id { get; set; }
            public string Country { get; set; }
            [JsonProperty(PropertyName = "city_name")]
            public string City { get; set; }
            public string Street { get; set; }
            public int ZipCode { get; set; }
        }


        [Fact]
        public async Task Query_with_nested_JsonPropertyName_inside_where_clause()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John", Address = new Address { City = "city1" } }, "users/1");
                    await session.StoreAsync(new User { Name = "Jane", Address = new Address { City = "city1" } }, "users/2");
                    await session.StoreAsync(new User { Name = "Tarzan", Address = new Address { City = "city2" } }, "users/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Where(x => x.Address.City == "city1");

                    Assert.Equal("from 'Users' where user_address.city_name = $p0", query.ToString());

                    var result = await query.ToListAsync();

                    Assert.Equal(2, result.Count);

                }
            }
        }

        [Fact]
        public async Task Query_with_nested_JsonPropertyName_inside_select_clause()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John", Address = new Address { City = "city1" } }, "users/1");
                    await session.StoreAsync(new User { Name = "Jane", Address = new Address { City = "city1" } }, "users/2");
                    await session.StoreAsync(new User { Name = "Tarzan", Address = new Address { City = "city2" } }, "users/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Select(x => new { CityName = x.Address.City });

                    Assert.Equal("from 'Users' select user_address.city_name as CityName", query.ToString());

                    var result = await query.ToListAsync();

                    Assert.Equal(3, result.Count);
                    Assert.Equal("city1", result[0].CityName);
                    Assert.Equal("city1", result[1].CityName);
                    Assert.Equal("city2", result[2].CityName);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task Query_with_nested_JsonPropertyName_inside_js_projection(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John", Address = new Address { City = "city1" } }, "users/1");
                    await session.StoreAsync(new User { Name = "Jane", Address = new Address { City = "city1" } }, "users/2");
                    await session.StoreAsync(new User { Name = "Tarzan", Address = new Address { City = "city2" } }, "users/3");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Select(x => new
                        {
                            CityName = x.Address.City,
                            Foo = "foo" + x.Age
                        });

                    Assert.Contains("CityName : ((x?.user_address)?.city_name)", query.ToString());

                    var result = await query.ToListAsync();

                    Assert.Equal(3, result.Count);
                    Assert.Equal("city1", result[0].CityName);
                    Assert.Equal("city1", result[1].CityName);
                    Assert.Equal("city2", result[2].CityName);
                }
            }
        }

    }


}
