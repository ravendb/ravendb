// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4091.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4091 : RavenTest
    {
        [Fact]
        public void can_get_in_progress_operation_when_deleting_by_index()
        {
            using (var documentStore = NewDocumentStore())
            {
                put_1500_companies(documentStore);

                WaitForIndexing(documentStore);

                var operation = documentStore.DatabaseCommands.DeleteByIndex(new RavenDocumentsByEntityName().IndexName, new IndexQuery
                {
                    Query = "Tag:[[Companies]]"
                });

                var progresses = new List<BulkOperationProgress>();

                operation.OnProgressChanged += progress =>
                {
                    progresses.Add(progress);
                };

                operation.WaitForCompletion();
                
                Assert.NotEmpty(progresses);

                Assert.Equal(1500, progresses.Last().ProcessedEntries);
                Assert.Equal(1500, progresses.Last().TotalEntries);
            }
        }

        [Fact]
        public void can_get_in_progress_operation_when_patching()
        {
            using (var documentStore = NewDocumentStore())
            {
                put_1500_companies(documentStore);

                WaitForIndexing(documentStore);

                var operation = documentStore.DatabaseCommands.UpdateByIndex(new RavenDocumentsByEntityName().IndexName, new IndexQuery()
                {
                    Query = "Tag:[[Companies]]"
                }, new[]
                {
                    new PatchRequest
                    {
                        Type = PatchCommandType.Add,
                        Name = "Sample",
                        Value = "Value"
                    },
                });

                var progresses = new List<BulkOperationProgress>();

                operation.OnProgressChanged += progress =>
                {
                    progresses.Add(progress);
                };

                operation.WaitForCompletion();

                Assert.NotEmpty(progresses);

                Assert.Equal(1500, progresses.Last().ProcessedEntries);
                Assert.Equal(1500, progresses.Last().TotalEntries);
            }
        }

        [Fact]
        public void can_get_in_progress_operation_when_patching_by_script()
        {
            using (var documentStore = NewDocumentStore())
            {
                put_1500_companies(documentStore);

                WaitForIndexing(documentStore);

                var operation = documentStore.DatabaseCommands.UpdateByIndex(new RavenDocumentsByEntityName().IndexName, new IndexQuery()
                {
                    Query = "Tag:[[Companies]]"
                }, new ScriptedPatchRequest
                {
                    Script = @"this.Sample = 'Value'"
                });

                var progresses = new List<BulkOperationProgress>();

                operation.OnProgressChanged += progress =>
                {
                    progresses.Add(progress);
                };

                operation.WaitForCompletion();

                Assert.NotEmpty(progresses);

                Assert.Equal(1500, progresses.Last().ProcessedEntries);
                Assert.Equal(1500, progresses.Last().TotalEntries);
            }
        }

        private static void put_1500_companies(EmbeddableDocumentStore documentStore)
        {
            using (var session = documentStore.OpenSession())
            {
                for (int i = 0; i < 1500; i++)
                {
                    session.Store(new Company {Name = $"Company {i}"});
                }

                session.SaveChanges();
            }
        }
    }
}