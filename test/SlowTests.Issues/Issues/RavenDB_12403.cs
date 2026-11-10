using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using CsvHelper;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Extensions;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12403 : RavenTestBase
    {
        public RavenDB_12403(ITestOutputHelper output) : base(output)
        {
        }

        private char a = 'a';
        private char z = 'z';

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public async Task Can_Export_raw_index_entries_in_Csv_Async_Corax(Options options) => await Can_Export_raw_index_entries_in_Csv_Async(options, csv =>
        {
            csv.Read();
            var value = csv.GetString(0);
            Assert.Equal("id()", value);
            value = csv.GetString(3);
            Assert.Equal("CoolCount", value);
            value = csv.GetString(2);
            Assert.Equal("LastName", value);
            value = csv.GetString(1);
            Assert.Equal("Name", value);
            
            var k = 0;
            a = 'a';
            z = 'z';
            while (csv.Read())
            {
                value = csv.GetString(3);
                Assert.Equal($"{(k * 2)}", value);
                value = csv.GetString(2);
                Assert.Equal($"{z}", value);
                value = csv.GetString(1);
                Assert.Equal($"{a}", value);
                value = csv.GetString(0);
                Assert.Equal($"user/{k}", value);
                k++;
                a++;
                z--;
            }
        });
        
        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public async Task Can_Export_raw_index_entries_in_Csv_Async_Lucene(Options options) => await Can_Export_raw_index_entries_in_Csv_Async(options, csv =>
        {
            csv.Read();
            var value = csv.GetString(0);
            Assert.Equal("CoolCount", value);
            value = csv.GetString(1);
            Assert.Equal("LastName", value);
            value = csv.GetString(2);
            Assert.Equal("Name", value);
            value = csv.GetString(3);
            Assert.Equal("id()", value);

            var k = 0;
            a = 'a';
            z = 'z';
            while (csv.Read())
            {
                value = csv.GetString(0);
                Assert.Equal($"{(k * 2)}", value);
                value = csv.GetString(1);
                Assert.Equal($"{z}", value);
                value = csv.GetString(2);
                Assert.Equal($"{a}", value);
                value = csv.GetString(3);
                Assert.Equal($"user/{k}", value);
                k++;
                a++;
                z--;
            }
        });
        
        private async Task Can_Export_raw_index_entries_in_Csv_Async(Options options, Action<CsvReader> assertionOfFile)
        {
            using (var store = GetDocumentStore(options))
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

                    await Indexes.WaitForIndexingAsync(store);
                }

                using var client = new HttpClient().WithConventions(store.Conventions);
                var stream = await client.GetStreamAsync(
                    $"{store.Urls[0]}/databases/{store.Database}/streams/queries?query=from index \'Users/CoolCount\'&format=csv&debug=entries");
                TextReader tr = new StreamReader(stream);
                var csv = CsvReader.Create(tr, new CsvReaderOptions { Separator = ',', HasHeader = false });
                assertionOfFile(csv);
            }
        }
        // RavenDB-12337
        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task Can_Export_stored_index_fields_only_in_Csv(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                    await Indexes.WaitForIndexingAsync(store);
                }
                var client = new HttpClient().WithConventions(store.Conventions);
                var stream = await client.GetStreamAsync(
                    $"{store.Urls[0]}/databases/{store.Database}/streams/queries?query=from index \'Users/CoolCount\' select __all_stored_fields&format=csv");
                TextReader tr = new StreamReader(stream);
                var csv = CsvReader.Create(tr, new CsvReaderOptions { Separator = ',', HasHeader = false });
                await csv.ReadAsync();
                var value = csv.GetString(0);
                Assert.Equal("@id", value);
                value = csv.GetString(1);
                Assert.Equal("CoolCount", value);
                var k = 0;
                while (await csv.ReadAsync())
                {
                    value = csv.GetString(0);
                    Assert.Equal($"user/{k}", value);
                    value = csv.GetString(1);
                    Assert.Equal($"{(k * 2)}", value);
                    k++;
                }
            }
        }
    }
}
