using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using FastTests.Server.Documents.Indexing;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_7940 : RavenTestBase
    {
        public class Person_ByName_1 : AbstractIndexCreationTask<IndexMerging.Person>
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
        public async void RecreatingIndexesToAReCreatedDatabase()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new RavenTestBase.Options
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
                
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, false)).ConfigureAwait(false);
                SpinWait.SpinUntil(() =>
                {
                    try
                    {
                        store.Maintenance.Send(new GetStatisticsOperation());
                        return false;
                    }
                    catch (DatabaseDisabledException)
                    {
                        return false;
                    }
                    catch (DatabaseDoesNotExistException)
                    {
                        return true;
                    }
                });

                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord
                {
                    DatabaseName = store.Database,
                    Settings = new Dictionary<string, string>
                    {
                        {"DataDir", path},
                        {"RunInMemory" , false.ToString()}
                    }
                }));

                SpinWait.SpinUntil(() =>
                {
                    var stats = store.Maintenance.Send(new GetStatisticsOperation());
                    return stats.Indexes.Length > 0;
                }, TimeSpan.FromSeconds(10));

                indexes = store.Maintenance.Send(new GetIndexesOperation(0, 128));
                Assert.True(indexes.Length > 0);

            }
        }
    }
}
