using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;

using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class TermVectorsInMultiMapIndexes : RavenTestBase
	{
		[Fact]
		public void CanCreateTermVectorInIndex()
		{
			var indexDefinition = new SampleData_Index().CreateIndexDefinition();
			Assert.Equal(FieldTermVector.WithPositionsAndOffsets, indexDefinition.TermVectors.Single().Value);
		}
	
		[Fact]
		public void CanCreateTermVectorInMultimapIndex()
		{
			var indexDefinition = new SampleData_MultiMapIndex() { Conventions = new DocumentConvention() }.CreateIndexDefinition();
			// note also that overriden CreateIndexDefinition in AbstractMultiMapIndexCreationTask<T> does not default Conventions property in the same way as AbstractIndexCreationTask<T>
			// not sure if this is by design!
			Assert.Equal(FieldTermVector.WithPositionsAndOffsets, indexDefinition.TermVectors.Single().Value);
		}

		public class SampleData
		{
			public string Name { get; set; }
		}

		public class SampleData_MultiMapIndex : AbstractMultiMapIndexCreationTask<SampleData>
		{
			public SampleData_MultiMapIndex()
			{
				AddMap<SampleData>(docs => from doc in docs
										   select new { doc.Name });
				TermVector(x => x.Name, FieldTermVector.WithPositionsAndOffsets);
			}
		}

		public class SampleData_Index : AbstractIndexCreationTask<SampleData>
		{
			public SampleData_Index()
			{
				Map = docs => from doc in docs
							  select new { doc.Name };
				TermVector(x => x.Name, FieldTermVector.WithPositionsAndOffsets);
			}
		}
	}

	// this is just to make VS compile the console app
	class Program
	{
		static void Main(string[] args)
		{
		}
	}
}
