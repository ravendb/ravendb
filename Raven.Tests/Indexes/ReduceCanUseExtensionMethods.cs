using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class ReduceCanUseExtensionMethods : RavenTest
	{
		private class PainfulInputData
		{
			public string Name;
			public string Tags;
		}

		private class IndexedFields
		{
			public string[] Tags;
		}

		[Fact]
		public void CanUseExtensionMethods()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Hi", new IndexDefinitionBuilder<PainfulInputData, IndexedFields>()
				{
					Map = documents => from doc in documents
					                   let tags = ((string[]) doc.Tags.Split(',')).Select(s => s.Trim()).Where(s => !string.IsNullOrEmpty(s))
					                   select new IndexedFields()
					                   {
						                   Tags = tags.ToArray()
					                   }
				});

				using (var session = store.OpenSession())
				{
					session.Store(new PainfulInputData()
					{
						Name = "Hello, universe",
						Tags = "Little, orange, comment"
					});

					session.Store(new PainfulInputData()
					{
						Name = "Highlander",
						Tags = "only-one"
					});

					session.SaveChanges();
				}

				while (store.DocumentDatabase.Statistics.StaleIndexes.Length > 0)
					Thread.Sleep(100);
				Assert.Empty(store.DocumentDatabase.Statistics.Errors);

				using (var session = store.OpenSession())
				{
					var results = session.Query<IndexedFields>("Hi")
						.Search(d => d.Tags, "only-one")
						.As<PainfulInputData>()
						.ToList();

					Assert.Single(results);
				}
			}
		}

		[Fact]
		public void CorrectlyUseExtensionMethodsOnConvertedType()
		{
			var indexDefinition = new PainfulIndex { Conventions = new DocumentConvention() }.CreateIndexDefinition();
			Assert.Contains("((String[]) doc.Tags.Split(", indexDefinition.Map);
		}

		private class PainfulIndex : AbstractMultiMapIndexCreationTask<IndexedFields>
		{
			public PainfulIndex()
			{
				AddMap<PainfulInputData>(documents => from doc in documents
										 // Do not remove the redundant (string[]). 
										 // It's intentional here and intended to test the following parsing: ((string[])prop).Select(...)
										 let tags = ((string[])doc.Tags.Split(',')).Select(s => s.Trim()).Where(s => !string.IsNullOrEmpty(s))
										 select new IndexedFields()
										 {
											 Tags = tags.ToArray()
										 });

			}
		}
	}
}