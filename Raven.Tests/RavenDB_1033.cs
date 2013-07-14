// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1033.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Bugs.Identifiers;
using Xunit;

namespace Raven.Tests
{
	public class RavenDB_1033 : IisExpressTestClient
	{
		class Item
		{
			public int Number { get; set; }
			public bool Active { get; set; }
		}

		private readonly IDocumentStore store;

		public RavenDB_1033()
		{
			store = NewDocumentStore(	
				fiddler:false,
				settings: new Dictionary<string, string>
				{
					{ "Raven/HttpCompression", "true" } // HttpCompression is enabled by default, just in case of changing it in the future
				}); 
		}

		[IISExpressInstalledFact]
		public void High_level_query_streaming_should_work_on_IIS_with_HttpCompression_enabled()
		{
			new RavenDocumentsByEntityName().Execute(store);

			using (var session = store.OpenSession())
			{
				for (int i = 0; i < 1500; i++)
				{
					session.Store(new Item());
				}
				session.SaveChanges();
			}

			WaitForIndexing(store);

			using (var session = store.OpenSession())
			{
				var enumerator = session.Advanced.Stream(session.Query<Item>(new RavenDocumentsByEntityName().IndexName));
				int count = 0;
				while (enumerator.MoveNext())
				{
					Assert.IsType<Item>(enumerator.Current.Document);
					count++;
				}

				Assert.Equal(1500, count);
			}
		}

		[IISExpressInstalledFact]
		public void Low_level_query_streaming_should_work_on_IIS_with_HttpCompression_enabled()
		{
			new RavenDocumentsByEntityName().Execute(store);

			using (var session = store.OpenSession())
			{
				for (int i = 0; i < 1500; i++)
				{
					session.Store(new Item());
				}
				session.SaveChanges();
			}

			WaitForIndexing(store);

			QueryHeaderInformation queryHeaders;
			var enumerator = store.DatabaseCommands.StreamQuery(new RavenDocumentsByEntityName().IndexName, new IndexQuery
			{
				Query = "",
				SortedFields = new[] { new SortedField(Constants.DocumentIdFieldName), }
			}, out queryHeaders);

			Assert.Equal(1500, queryHeaders.TotalResults);

			int count = 0;
			while (enumerator.MoveNext())
			{
				count++;
			}

			Assert.Equal(1500, count);
		}


		[IISExpressInstalledFact]
		public void Low_level_document_streaming_should_work_on_IIS_with_HttpCompression_enabled()
		{
			using (var session = store.OpenSession())
			{
				for (int i = 0; i < 1500; i++)
				{
					session.Store(new Item());
				}
				session.SaveChanges();
			}

			int count = 0;
			using (var streamDocs = store.DatabaseCommands.StreamDocs(startsWith: "items/"))
			{
				while (streamDocs.MoveNext())
				{
					count++;
				}
			}
			Assert.Equal(1500, count);
		}

		[IISExpressInstalledFact]
		public void High_level_document_streaming_should_work_on_IIS_with_HttpCompression_enabled()
		{
			using (var session = store.OpenSession())
			{
				for (int i = 0; i < 1500; i++)
				{
					session.Store(new Item());
				}
				session.SaveChanges();
			}

			int count = 0;
			using (var session = store.OpenSession())
			{
				using (var reader = session.Advanced.Stream<Item>(startsWith: "items/"))
				{
					while (reader.MoveNext())
					{
						count++;
						Assert.IsType<Item>(reader.Current.Document);
					}
				}
			}
			Assert.Equal(1500, count);
		}

		public void Disponse()
		{
			store.Dispose();
			base.Dispose();
		}
	}
}