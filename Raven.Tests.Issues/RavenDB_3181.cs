using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_3181 : RavenTestBase
    {
        public class Item
        {
        }


        [Theory]
        [InlineData("esent")]
        [InlineData("voron")]
        public void CheckIfDocumentIsCompressed(string storage)
        {
            using (var store = NewDocumentStore(runInMemory: false, requestedStorage: storage))
            {
                var newDataPath = base.NewDataPath();

                store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "RavenDB_3181",
                    Settings =
                    {
                        {"Raven/ActiveBundles", "Compression"},
                        {"Raven/DataDir", newDataPath}
                    }
                });

                using (var s = store.OpenSession("RavenDB_3181"))
                {
                    s.Store(new Item(), "items/1");
                    s.SaveChanges();
                }

                store.DatabaseCommands.GlobalAdmin.DeleteDatabase("RavenDB_3181", hardDelete: false);

                store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                {
                    Id = "RavenDB_3181",
                    Settings =
                    {
                        {"Raven/DataDir", newDataPath}
                    }
                });


                using (var s = store.OpenSession("RavenDB_3181"))
                {
                    var e = Assert.Throws<ErrorResponseException>(() => s.Load<Item>(1));
                    Assert.Contains("Document 'items/1' is compressed, but the compression bundle is not enabled.", e.Message);
                }

                store.DatabaseCommands.GlobalAdmin.DeleteDatabase("RavenDB_3181", hardDelete: true);

            }
        }
    }

}
