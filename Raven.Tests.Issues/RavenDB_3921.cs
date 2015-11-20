// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3921.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Database;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3921 : RavenTest
    {
        [Fact]
        public void needs_to_mark_document_as_non_authoritative_if_dtc_transaction_was_being_in_progress_before_transactional_batch_has_opened()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent"))
            {
                var database = store.DocumentDatabase;

                database.Documents.Put("items/1", null, new RavenJObject { { "Value", "1" } }, new RavenJObject(), null);

                database.Documents.Put("items/1", null, new RavenJObject { { "Value", "2" } }, new RavenJObject(), new TransactionInformation
                                                                                                                                {
                                                                                                                                    Id = "1",
                                                                                                                                    Timeout = TimeSpan.FromMinutes(1)
                                                                                                                                });
                database.TransactionalStorage.Batch(accessor => // DTC transaction still in progress
                {
                    database.PrepareTransaction("1");
                    database.Commit("1");

                    var behavior = database.InFlightTransactionalState.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, "items/1");

                    Assert.NotNull(behavior); // should not be null because DTC transaction was still in progress at the moment of opening the batch

                    var doc = behavior(accessor.Documents.DocumentByKey("items/1"));

                    Assert.True(doc.NonAuthoritativeInformation.Value);
                });
            }
        }
    }
}