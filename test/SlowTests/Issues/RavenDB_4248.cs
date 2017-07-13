// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4248.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Transformers;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4248 : RavenTestBase
    {
        private class SimpleTransformer : AbstractTransformerCreationTask
        {
            public class Result
            {
                public string Name { get; set; }
            }

            public override TransformerDefinition CreateTransformerDefinition(bool prettify = true)
            {
                return new TransformerDefinition
                {
                    Name = "SimpleTransformer",
                    TransformResults = "from r in results select new { Name = Parameter(\"Name\") }"
                };
            }
        }

        [Fact]
        public void CanUseTransformerInStreamDocs_Session()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "John" });
                    session.Store(new Person { Name = "George" });

                    session.SaveChanges();
                }

                var transformer = new SimpleTransformer();
                transformer.Execute(store);

                using (var session = store.OpenSession())
                {
                    using (var enumerator = session.Advanced.Stream<SimpleTransformer.Result>(startsWith:"people/", transformer: transformer.TransformerName, transformerParameters: new Dictionary<string, object> { { "Name", "Test" } }))
                    {
                        var count = 0;
                        while (enumerator.MoveNext())
                        {
                            var result = enumerator.Current;

                            Assert.Equal("Test", result.Document.Name);

                            count++;
                        }

                        Assert.Equal(2, count);
                    }

                    using (var enumerator = session.Advanced.Stream<SimpleTransformer.Result>(fromChangeVector:null, transformer: transformer.TransformerName, transformerParameters: new Dictionary<string, object> { { "Name", "Test" } }))
                    {
                        var count = 0;
                        while (enumerator.MoveNext())
                        {
                            var result = enumerator.Current;

                            Assert.Equal("Test", result.Document.Name);

                            count++;
                        }

                        Assert.Equal(2 + 1, count); // +1 for HiLo
                    }
                }
            }
        }

        [Fact]
        public async Task CanUseTransformerInStreamDocs_Session_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "John" });
                    session.Store(new Person { Name = "George" });

                    session.SaveChanges();
                }

                var transformer = new SimpleTransformer();
                transformer.Execute(store);

                using (var session = store.OpenAsyncSession())
                {
                    using (var enumerator = await session.Advanced.StreamAsync<SimpleTransformer.Result>(startsWith:"people/", transformer: transformer.TransformerName, transformerParameters: new Dictionary<string, object> { { "Name", "Test" } }))
                    {
                        var count = 0;
                        while (await enumerator.MoveNextAsync())
                        {
                            var result = enumerator.Current;

                            Assert.Equal("Test", result.Document.Name);

                            count++;
                        }

                        Assert.Equal(2, count);
                    }

                    using (var enumerator = await session.Advanced.StreamAsync<SimpleTransformer.Result>(fromChangeVector:null, transformer: transformer.TransformerName, transformerParameters: new Dictionary<string, object> { { "Name", "Test" } }))
                    {
                        var count = 0;
                        while (await enumerator.MoveNextAsync())
                        {
                            var result = enumerator.Current;

                            Assert.Equal("Test", result.Document.Name);

                            count++;
                        }

                        Assert.Equal(2 + 1, count); // +1 for HiLo
                    }
                }
            }
        }
    }
}