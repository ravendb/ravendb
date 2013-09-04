using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class IndexLocking : RavenTest
	{
		[Fact]
		public void LockingIndexesInMemoryWillNotFail()
		{
			using (var store = NewDocumentStore(runInMemory: true))
			using(var session = store.OpenSession())
			{
				var index = new IndexSample
				{
					Conventions = new DocumentConvention()
				};

				index.Execute(store);

				var indexDefinition = store.DocumentDatabase.IndexDefinitionStorage.GetIndexDefinition("IndexSample");

				var definition = indexDefinition.Clone();
				definition.LockMode = IndexLockMode.LockedIgnore;
				store.DocumentDatabase.IndexDefinitionStorage.UpdateIndexDefinitionWithoutUpdatingCompiledIndex(definition);

				indexDefinition = store.DocumentDatabase.IndexDefinitionStorage.GetIndexDefinition("IndexSample");

				Assert.Equal(indexDefinition.LockMode, IndexLockMode.LockedIgnore);
			}
		}
	}

	public class IndexSample : AbstractIndexCreationTask<Contact>
	{
		public IndexSample()
		{
			Map = contacts =>
				from contact in contacts
				select new
				{
					contact.FirstName,
					PrimaryEmail_EmailAddress = contact.PrimaryEmail.Email,
				};
		}
	}
}
