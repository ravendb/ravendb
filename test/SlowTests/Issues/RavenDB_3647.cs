using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3647 : RavenTestBase
    {
        [Fact]
        public void CanLockTransformers()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteTransformer(new SimpleTransformer());
                //Checking that we can lock transformer
                store.Admin.Send(new SetTransformerLockOperation("SimpleTransformer", TransformerLockMode.LockedIgnore));
                var transformerDefinition = store.Admin.Send(new GetTransformerOperation("SimpleTransformer"));
                var oldTransformResults = transformerDefinition.TransformResults;
                Assert.Equal(transformerDefinition.LockMode, TransformerLockMode.LockedIgnore);
                //Checking that we can't change a locked transformer
                transformerDefinition.TransformResults = NewTransformResults;
                store.Admin.Send(new PutTransformerOperation(transformerDefinition));
                transformerDefinition = store.Admin.Send(new GetTransformerOperation("SimpleTransformer"));
                Assert.Equal(transformerDefinition.TransformResults, oldTransformResults);
                //Checking that we can unlock a transformer
                store.Admin.Send(new SetTransformerLockOperation("SimpleTransformer", TransformerLockMode.Unlock));
                transformerDefinition = store.Admin.Send(new GetTransformerOperation("SimpleTransformer"));
                Assert.Equal(transformerDefinition.LockMode, TransformerLockMode.Unlock);
                //checking that the transformer is indeed overridden
                transformerDefinition.TransformResults = NewTransformResults;
                store.Admin.Send(new PutTransformerOperation(transformerDefinition));
                transformerDefinition = store.Admin.Send(new GetTransformerOperation("SimpleTransformer"));
                Assert.Equal(NewTransformResults, transformerDefinition.TransformResults);
            }
        }

        [Fact]
        public void CanLockIndexes()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new SimpleIndex());
                //Checking that we can lock index
                store.Admin.Send(new SetIndexLockOperation("SimpleIndex", IndexLockMode.LockedIgnore));
                var indexDefinition = store.Admin.Send(new GetIndexOperation("SimpleIndex"));
                var map = indexDefinition.Maps.First();
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.LockedIgnore);
                //Checking that we can't change a locked index
                indexDefinition.Maps = new HashSet<string> { NewMap };
                store.Admin.Send(new PutIndexesOperation(indexDefinition));
                WaitForIndexing(store);
                indexDefinition = store.Admin.Send(new GetIndexOperation("SimpleIndex"));
                Assert.Equal(indexDefinition.Maps.First(), map);
                //Checking that we can unlock a index
                store.Admin.Send(new SetIndexLockOperation("SimpleIndex", IndexLockMode.Unlock));
                indexDefinition = store.Admin.Send(new GetIndexOperation("SimpleIndex"));
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.Unlock);
                //checking that the index is indeed overridden
                indexDefinition.Maps = new HashSet<string> { NewMap };
                store.Admin.Send(new PutIndexesOperation(indexDefinition));
                WaitForIndexing(store);
                indexDefinition = store.Admin.Send(new GetIndexOperation("SimpleIndex"));
                Assert.Equal(NewMap, indexDefinition.Maps.First());
            }
        }

        private const string NewTransformResults = "from result in results select new { Number = result.Number + int.MaxValue };";

        private class SimpleTransformer : AbstractTransformerCreationTask<SimpleData>
        {
            public SimpleTransformer()
            {
                TransformResults = results => from result in results
                                              select new { Number = result.Number ^ int.MaxValue };
            }
        }

        private class SimpleData
        {
            public string Id { get; set; }

            public int Number { get; set; }
        }

        private const string NewMap = "from doc in docs.SimpleDatas select new { Id = doc.Id, Number = doc.Number };";

        private class SimpleIndex : AbstractIndexCreationTask<SimpleData>
        {
            public SimpleIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Number
                              };
            }
        }
    }
}
