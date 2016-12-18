// -----------------------------------------------------------------------
//  <copyright file="RavenDB2537.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.MailingList;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB2537 : RavenTest
    {
        [Fact]
        public void ShutdownDatabaseDuringPreparedTransaction()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent", runInMemory: false))
            {
                var transactionInformation = new TransactionInformation
                {
                    Id = "tx",
                    Timeout = TimeSpan.FromHours(1)
                };
                store.SystemDatabase.Documents.Put("test", null, new RavenJObject{{"Exists", true}}, new RavenJObject(), transactionInformation);

                Assert.False(store.SystemDatabase.Documents.Get("test", null).DataAsJson.Value<bool>("Exists"));

                store.SystemDatabase.PrepareTransaction("tx");

                Assert.False(store.SystemDatabase.Documents.Get("test", null).DataAsJson.Value<bool>("Exists"));
            }

            using (var store = NewDocumentStore(requestedStorage: "esent", runInMemory: false))
            {
                // can be loaded again
            }
        }
    }
}
