using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3647 : RavenTest
    {
        [Fact]
        public void CanLockTransformers()
        {
            using (var store = NewDocumentStore())
            {
                store.ExecuteTransformer(new SimpleTransformer());
                //Checking that we can lock transformer
                store.DatabaseCommands.SetTransformerLock("SimpleTransformer", TransformerLockMode.LockedIgnore);
                var transformerDefinition = store.DatabaseCommands.GetTransformer("SimpleTransformer");
                var oldTransformResults = transformerDefinition.TransformResults;
                Assert.Equal(transformerDefinition.LockMode, TransformerLockMode.LockedIgnore);
                //Checking that we can't change a locked transformer
                transformerDefinition.TransformResults = NewTransformResults;
                store.DatabaseCommands.PutTransformer("SimpleTransformer", transformerDefinition);
                transformerDefinition = store.DatabaseCommands.GetTransformer("SimpleTransformer");
                Assert.Equal(transformerDefinition.TransformResults, oldTransformResults);
                //Checking that we can unlock a transformer
                store.DatabaseCommands.SetTransformerLock("SimpleTransformer", TransformerLockMode.Unlock);
                transformerDefinition = store.DatabaseCommands.GetTransformer("SimpleTransformer");
                Assert.Equal(transformerDefinition.LockMode, TransformerLockMode.Unlock);
                //checking that the transformer is indeed overridden
                transformerDefinition.TransformResults = NewTransformResults;
                store.DatabaseCommands.PutTransformer("SimpleTransformer", transformerDefinition);
                transformerDefinition = store.DatabaseCommands.GetTransformer("SimpleTransformer");
                Assert.Equal(NewTransformResults, transformerDefinition.TransformResults);
            }
        }

        [Fact]
        public void CanLockIndexes()
        {
            using (var store = NewDocumentStore())
            {
                store.ExecuteIndex(new SimpleIndex());
                //Checking that we can lock index
                store.DatabaseCommands.SetIndexLock("SimpleIndex", IndexLockMode.LockedIgnore);
                var indexDefinition = store.DatabaseCommands.GetIndex("SimpleIndex");
                var map = indexDefinition.Map;
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.LockedIgnore);
                //Checking that we can't change a locked index
                indexDefinition.Map = NewMap;
                store.DatabaseCommands.PutIndex("SimpleIndex", indexDefinition, true);
                indexDefinition = store.DatabaseCommands.GetIndex("SimpleIndex");
                Assert.Equal(indexDefinition.Map, map);
                //Checking that we can unlock a index
                store.DatabaseCommands.SetIndexLock("SimpleIndex", IndexLockMode.Unlock);
                indexDefinition = store.DatabaseCommands.GetIndex("SimpleIndex");
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.Unlock);
                //checking that the index is indeed overridden
                indexDefinition.Map = NewMap;
                store.DatabaseCommands.PutIndex("SimpleIndex", indexDefinition, true);
                indexDefinition = store.DatabaseCommands.GetIndex("SimpleIndex");
                Assert.Equal(NewMap, indexDefinition.Map);
            }
        }

        private const string NewTransformResults = "from result in results  select new { Number = result.Number + int.MaxValue };";

        public class SimpleTransformer : AbstractTransformerCreationTask<SimpleData>
        {
            public SimpleTransformer()
            {
                TransformResults = results => from result in results
                                              select new { Number = result.Number ^ int.MaxValue };
            }
        }

        public class SimpleData
        {
            public string Id { get; set; }

            public int Number { get; set; }
        }

        private const string NewMap = "from doc in docs.SimpleDatas select new { Id = doc.Id, Number = doc.Number };";

        public class SimpleIndex : AbstractIndexCreationTask<SimpleData>
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
