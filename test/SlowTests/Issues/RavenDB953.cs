// -----------------------------------------------------------------------
//  <copyright file="RavenDB953.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB953 : RavenTestBase
    {
        [Fact]
        public void PutTransformerAsyncSucceedsWhenExistingDefinitionHasError()
        {
            using (var store = GetDocumentStore())
            {
                // put an index containing an intentional mistake (calling LoadDocument is not permitted in TransformResults)
                store.Admin.Send(new PutTransformerOperation(new TransformerDefinition
                {
                    Name = "test",
                    TransformResults = "from result in results select new {Doc = LoadDocument(result.Id)}"
                }));

                using (var s = store.OpenSession())
                {
                    var entity = new { Id = "MyId" };
                    s.Store(entity);
                    s.SaveChanges();

                    try
                    {
                        s.Advanced.DocumentQuery<dynamic>("test")
                            .WaitForNonStaleResultsAsOfNow()
                            .WhereLucene("Id", "MyId")
                            .ToList();
                    }
                    catch
                    {
                        // ignore - we know it will fail
                    }
                }

                // now try to put the correct index definition
                store.Admin.SendAsync(new PutTransformerOperation(new TransformerDefinition
                {
                    Name = "test",
                    TransformResults = "from result in results select new {Doc = LoadDocument(result.Id)}"
                })).Wait();
            }
        }
    }
}