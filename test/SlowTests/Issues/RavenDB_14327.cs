using System;
using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14327 : RavenLowLevelTestBase
    {
        public RavenDB_14327(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Three_consecutive_write_errors_should_error_index()
        {
            UseNewLocalServer();

            using (var db = CreateDocumentDatabase())
            {
                using (var index = MapIndex.CreateNew(new IndexDefinition()
                {
                    Name = "Users_ByName",
                    Maps =
                    {
                        "from user in docs.Users select new { user.Name }"
                    },
                    Type = IndexType.Map
                }, db))
                {
                    PutUser(db);

                    index._indexStorage.SimulateIndexWriteException = new FormatException();

                    index.Start();

                    Assert.True(SpinWait.SpinUntil(() => index.State == IndexState.Error, TimeSpan.FromMinutes(1)));
                }
            }
        }

        private static void PutUser(DocumentDatabase db)
        {
            using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                    {
                        ["Name"] = "John",
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            { [Constants.Documents.Metadata.Collection] = "Users" }
                    }))
                    {
                        db.DocumentsStorage.Put(context, "users/1", null, doc);
                    }

                    tx.Commit();
                }
            }
        }
    }
}
