// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3921.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3921 : RavenTest
    {
        [Fact]
        public void needs_to_mark_document_as_non_authoritative_if_dtc_transaction_was_in_progress_at_the_moment_of_opening_the_batch()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent"))
            {
                var database = store.DocumentDatabase;

                database.Documents.Put("items/1", null, new RavenJObject { { "Value", "1" } }, new RavenJObject(), null);

                database.Documents.Put("items/1", null, new RavenJObject { { "Value", "2" } }, new RavenJObject(), new TransactionInformation { Id = "1", Timeout = TimeSpan.FromMinutes(1) });
                var commitDtcTransaction = new Thread(() =>
                {
                    database.PrepareTransaction("1");
                    database.Commit("1");
                });

                database.TransactionalStorage.Batch(accessor => // DTC transaction still in progress
                {
                    commitDtcTransaction.Start();
                    commitDtcTransaction.Join();

                    var behavior = accessor.InFlightStateSnapshot.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, "items/1");

                    Assert.NotNull(behavior); // should not be null because DTC transaction was still in progress at the moment of opening the batch

                    var doc = behavior(accessor.Documents.DocumentByKey("items/1"));

                    Assert.True(doc.NonAuthoritativeInformation.Value);

                    database.TransactionalStorage.Batch(nestedAccessor => // even a nested batch has the same snapshot
                    {
                        // nested batch should have the same snapshot
                        Assert.Same(accessor.InFlightStateSnapshot, nestedAccessor.InFlightStateSnapshot);

                        // actually it should have the same accessor
                        Assert.Same(accessor, nestedAccessor);

                        behavior = nestedAccessor.InFlightStateSnapshot.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, "items/1");

                        Assert.NotNull(behavior); // should not be null because DTC transaction was still in progress at the moment of opening the parent batch

                        doc = behavior(nestedAccessor.Documents.DocumentByKey("items/1"));

                        Assert.True(doc.NonAuthoritativeInformation.Value);
                    });
                });
            }
        }
    }
}