using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class TermVectorsInMultiMapIndexes : RavenTestBase
    {
        [Fact]
        public void CanCreateTermVectorInIndex()
        {
            var indexDefinition = new SampleData_Index().CreateIndexDefinition();
            Assert.Equal(FieldTermVector.WithPositionsAndOffsets, indexDefinition.Fields.Single().Value.TermVector.Value);
        }
    
        [Fact]
        public void CanCreateTermVectorInMultimapIndex()
        {
            var indexDefinition = new SampleData_MultiMapIndex { Conventions = new DocumentConvention() }.CreateIndexDefinition();
            // note also that overriden CreateIndexDefinition in AbstractMultiMapIndexCreationTask<T> does not default Conventions property in the same way as AbstractIndexCreationTask<T>
            // not sure if this is by design!
            Assert.Equal(FieldTermVector.WithPositionsAndOffsets, indexDefinition.Fields.Single().Value.TermVector.Value);
        }

        private class SampleData
        {
            public string Name { get; set; }
        }

        private class SampleData_MultiMapIndex : AbstractMultiMapIndexCreationTask<SampleData>
        {
            public SampleData_MultiMapIndex()
            {
                AddMap<SampleData>(docs => from doc in docs
                                           select new { doc.Name });
                TermVector(x => x.Name, FieldTermVector.WithPositionsAndOffsets);
            }
        }

        private class SampleData_Index : AbstractIndexCreationTask<SampleData>
        {
            public SampleData_Index()
            {
                Map = docs => from doc in docs
                              select new { doc.Name };
                TermVector(x => x.Name, FieldTermVector.WithPositionsAndOffsets);
            }
        }
    }
}
