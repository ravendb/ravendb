using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_7940 : RavenTestBase
    {
        public RavenDB_7940(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string AddressId { get; set; }
        }

        private class Person_ByName_1 : AbstractIndexCreationTask<Person>
        {
            public Person_ByName_1()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     Name = p.Name
                                 };
            }
        }

        [Fact]
        public async Task RecreatingIndexesToARecreatedDatabase()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = x => Path.GetFileName(path),
                Path = path
            }))
            {
                var index = new Person_ByName_1();
                store.ExecuteIndex(index);

                var indexes = await store.Maintenance.SendAsync(new GetIndexesOperation(0, 128));
                Assert.True(indexes.Length > 0);

                // We want to keep the files on the disk

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, false));

                var sp = new Stopwatch();
                sp.Start();
                while (sp.Elapsed <= TimeSpan.FromSeconds(5))
                {
                    try
                    {
                        store.Maintenance.Send(new GetStatisticsOperation());
                    }
                    catch (DatabaseDisabledException)
                    {
                    }
                    catch (DatabaseDoesNotExistException)
                    {
                        break;
                    }
                }

                sp.Reset();

                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord
                {
                    DatabaseName = store.Database,
                    Settings = new Dictionary<string, string>
                    {
                        {"DataDir", path},
                        {"RunInMemory" , false.ToString()}
                    }
                }));


                sp.Start();
                while (sp.Elapsed <= TimeSpan.FromSeconds(10))
                {
                    var stats = store.Maintenance.Send(new GetStatisticsOperation());
                    if (stats?.Indexes.Length > 0)
                    {
                        break;
                    }

                }
                sp.Stop();

                indexes = store.Maintenance.Send(new GetIndexesOperation(0, 128));
                Assert.True(indexes.Length > 0);

            }
        }
    }
}
