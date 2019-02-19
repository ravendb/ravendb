using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using CsvHelper;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12403 : RavenTestBase
    {
        private char a = 'a';
        private char z = 'z';

        [Fact]
        public async Task Can_Export_raw_index_entries_in_Csv_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new User
                        {
                            Id = $"User/{i}",
                            Name = $"{a}",
                            LastName = $"{z}",
                            Count = i
                        });
                        a++;
                        z--;
                    }

                    session.SaveChanges();

                    await store.Maintenance.SendAsync(new PutIndexesOperation(new[]
                    {
                        new IndexDefinition
                        {
                            Maps =
                            {
                                @"from user in docs.Users
                                    select new
                                    {
                                        user.Name,
                                        user.LastName,
                                        CoolCount = user.Count * 2
                                    }"
                            },
                            Name = "Users/CoolCount"
                        }
                    }));

                    WaitForIndexing(store);
                }

                var client = new HttpClient();
                var stream = await client.GetStreamAsync(
                    $"{store.Urls[0]}/databases/{store.Database}/streams/queries?query=from index \'Users/CoolCount\'&format=csv&debug=entries");
                TextReader tr = new StreamReader(stream);
                var csv = new CsvReader(tr);

                csv.Read();
                csv.TryGetField(0, out string value);
                Assert.Equal("CoolCount", value);
                csv.TryGetField(1, out value);
                Assert.Equal("LastName", value);
                csv.TryGetField(2, out value);
                Assert.Equal("Name", value);
                csv.TryGetField(3, out value);
                Assert.Equal("id()", value);

                var k = 0;
                a = 'a';
                z = 'z';
                while (csv.Read())
                {
                    csv.TryGetField(0, out value);
                    Assert.Equal($"{(k * 2)}", value);
                    csv.TryGetField(1, out value);
                    Assert.Equal($"{z}", value);
                    csv.TryGetField(2, out value);
                    Assert.Equal($"{a}", value);
                    csv.TryGetField(3, out value);
                    Assert.Equal($"user/{k}", value);
                    k++;
                    a++;
                    z--;
                }
            }
        }
        // RavenDB-12337
        [Fact]
        public async Task Can_Export_stored_index_fields_only_in_Csv()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new User
                        {
                            Id = $"User/{i}",
                            Name = $"{a}",
                            LastName = $"{z}",
                            Count = i
                        });
                        a++;
                        z--;
                    }

                    session.SaveChanges();

                    await store.Maintenance.SendAsync(new PutIndexesOperation(new[]
                    {
                        new IndexDefinition
                        {
                            Maps =
                            {
                                @"from user in docs.Users
                                    select new
                                    {
                                        user.Name,
                                        user.LastName,
                                        CoolCount = user.Count * 2
                                    }"
                            },
                            Name = "Users/CoolCount",
                            Fields = new Dictionary<string, IndexFieldOptions>()
                            {
                                {
                                    "CoolCount", new IndexFieldOptions()
                                    {
                                        Storage = FieldStorage.Yes
                                    }
                                },
                            }
                        }
                    }));

                    WaitForIndexing(store);
                }
                var client = new HttpClient();
                var stream = await client.GetStreamAsync(
                    $"{store.Urls[0]}/databases/{store.Database}/streams/queries?query=from index \'Users/CoolCount\' select __all_stored_fields&format=csv");
                TextReader tr = new StreamReader(stream);
                var csv = new CsvReader(tr);
                csv.Read();
                csv.TryGetField(0, out string value);
                Assert.Equal("@id", value);
                csv.TryGetField(1, out value);
                Assert.Equal("CoolCount", value);
                var k = 0;
                while (csv.Read())
                {
                    csv.TryGetField(0, out value);
                    Assert.Equal($"user/{k}", value);
                    csv.TryGetField(1, out value);
                    Assert.Equal($"{(k * 2)}", value);
                    k++;
                }
            }
        }
    }
}
