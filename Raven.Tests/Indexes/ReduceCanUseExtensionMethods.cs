using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class ReduceCanUseExtensionMethods : RavenTest
	{
		public class PainfulInputData
		{
			public string Name;
			public string Tags;
		}

		public class IndexedFields
		{
			public string[] Tags;
		}

		[Fact]
		public void CanUseExtensionMethods()
		{
			using (var store = NewDocumentStore())
			{
				store.Initialize();

				store.DatabaseCommands.PutIndex("Hi", new IndexDefinitionBuilder<PainfulInputData, IndexedFields>()
					{
						Map = documents => from doc in documents
										   let tags = ((string[])doc.Tags.Split(',')).Select(s => s.Trim()).Where(s => !string.IsNullOrEmpty(s))
										   select new IndexedFields()
											   {
												   Tags = tags.ToArray()
											   }
					});

				using(var session = store.OpenSession())
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

				//  How to assert no indexing errors?  
				//  When I create a similur index, the UI shows an indexing error related to using .Select().

				using(var session = store.OpenSession())
				{
					var results = session.Query<IndexedFields>("Hi")
						.Customize(a => a.WaitForNonStaleResults())
						.Search(d => d.Tags, "only-one")
						.As<PainfulInputData>()
						.ToList();

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);

					Assert.Single(results);
				}
			}
		}
	
	}
}
