// -----------------------------------------------------------------------
//  <copyright file="DbShutDownMidTransaction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.DTC.NSB
{
    public class DbShutDownMidTransaction : RavenTest
    {
        public DbShutDownMidTransaction()
        {
            IOExtensions.DeleteDirectory("DbShutDownMidTransaction");
        }

        public override void Dispose()
        {
            base.Dispose();
            IOExtensions.DeleteDirectory("DbShutDownMidTransaction");
        }

        [Fact]
        public void WillAllowCommittingTransactionFromBeforeShutdown()
        {
            using (var store = NewDocumentStore(runInMemory: false, requestedStorage: "esent", dataDir: "DbShutDownMidTransaction"))
            {
                var tx = new TransactionInformation
                {
                    Id = "tx",
                    Timeout = TimeSpan.FromHours(1)
                };

                store.SystemDatabase.Documents.Put("test", null, new RavenJObject(), new RavenJObject(), tx);

                store.SystemDatabase.PrepareTransaction("tx");
            }

            using ( var store = NewDocumentStore(runInMemory: false, requestedStorage: "esent", dataDir: "DbShutDownMidTransaction"))
            {
                store.SystemDatabase.Commit("tx");


                Assert.NotNull(store.SystemDatabase.Documents.Get("test", null));
            }
        }
    }
}
