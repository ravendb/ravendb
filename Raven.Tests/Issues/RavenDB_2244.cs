// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2244.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
    using System.Transactions;

    using Raven.Abstractions.Commands;
    using Raven.Abstractions.Data;
    using Raven.Abstractions.Exceptions;
    using Raven.Json.Linq;

    using Xunit;

    public class RavenDB_2244 : RavenTest
    {
        [Fact]
        public void Does_not_throw_snapshot_error_on_transaction_patch_batch()
        {
            using (var documentStore = this.NewDocumentStore())
            {
                documentStore.DatabaseCommands.Put("Company/1", null, new RavenJObject(), new RavenJObject());

                using (new TransactionScope())
                {
                    Assert.Throws<ConcurrencyException>(
                        () =>
                        documentStore.DatabaseCommands.Batch(
                            new[]
                            {
                                new PatchCommandData
                                {
                                    Key = "Company/2",
                                    Patches =
                                        new[]
                                        { new PatchRequest { Name = "Property", Value = "Value" } },
                                    PatchesIfMissing =
                                        new[]
                                        { new PatchRequest { Name = "Property", Value = "Value" } },
                                    Metadata = new RavenJObject(),
                                },
                                new PatchCommandData
                                {
                                    Key = "Company/1",
                                    Patches =
                                        new[]
                                        { new PatchRequest { Name = "Property", Value = "Value" } },
                                    Etag = Etag.Empty,
                                }
                            }));
                }
            }
        }
    }
}