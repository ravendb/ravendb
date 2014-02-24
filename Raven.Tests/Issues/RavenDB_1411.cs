// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1411.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1411 : RavenTest
	{
		public class Foo
		{
			public string Id { get; set; } 
			public string Item { get; set; } 
		}

		public class Bar
		{
			public string Id { get; set; } 
			public string Item { get; set; } 
		}

		public class Baz
		{
			public string Id { get; set; }
			public string Item { get; set; }
		}

		public class SingleMapIndex : AbstractIndexCreationTask<Foo>
		{
			public SingleMapIndex()
			{
				Map = foos => from foo in foos select new {foo.Item};
			}
		}

		public class MultiMapIndex : AbstractMultiMapIndexCreationTask<MultiMapOutput>
		{
			public MultiMapIndex()
			{
				AddMap<Foo>(foos => from foo in foos select new {foo.Item});
				AddMap<Bar>(bars => from bar in bars select new {bar.Item});
			}
		}

		public class MultiMapOutput
		{
			public string Item { get; set; }
		}

		[Fact]
		public void ShouldWork_Optimization_NewIndexedWillGetPrecomputedDocumentsToIndexToAvoidRetrievingFromDisk()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						session.Store(new Foo
						{
							Item = "Ball/" + i
						});

						session.Store(new Bar
						{
							Item = "Computer/" + i
						});
					}

					for (int i = 0; i < 10000; i++)
					{
						session.Store(new Baz
						{
							Item = "Baz/" + i
						});
					}

					session.SaveChanges();
				}

				WaitForIndexing(store.DocumentDatabase);

				new SingleMapIndex().Execute(store);
				new MultiMapIndex().Execute(store);

				WaitForIndexing(store.DocumentDatabase);

				using (var session = store.OpenSession())
				{
					var count1 = session.Query<Foo, SingleMapIndex>().Count();
					Assert.Equal(10, count1);

					var count2 = session.Query<MultiMapOutput, MultiMapIndex>().Count();
					Assert.Equal(20, count2);
				}
			}
		}
	}
}