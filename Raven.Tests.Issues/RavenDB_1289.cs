// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1289.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1289 : RavenTest
	{
		public class FooBarDto
		{
			public string Name { get; set; }
			public string[] Bars { get; set; }
			public string Input { get; set; }
		}

		public class FooCapitalNameDto
		{
			public string Name { get; set; }
			public string Input { get; set; }
		}

		public class Foo
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string[] Bars { get; set; }
		}

		public class Bar
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class FooBarTransformer : AbstractTransformerCreationTask<Foo>
		{
			public FooBarTransformer()
			{
				TransformResults = foos =>
								   from foo in foos
								   select new
								   {
									   Name = foo.Name,
									   Bars =
									   foo.Bars == null
										   ? new string[0]
										   : foo.Bars.Select(x => LoadDocument<Bar>(x).Name),
									   Input = Query("input")
								   };
			}
		}

		public class FooCapitalNameTransformer : AbstractTransformerCreationTask<Foo>
		{
			public FooCapitalNameTransformer()
			{
				TransformResults = foos =>
								   from foo in foos
								   select new
								   {
									   Name = foo.Name.ToUpper(),
									   Input = Query("input")
								   };
			}
		}

		[Fact]
		public void CanUseResultTransformerWithStartWith_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				DoTest(store);
			}
		}

		[Fact]
		public void CanUseResultTransformerWithStartWith_Remote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DoTest(store);
			}
		}

		[Fact]
		public void CanUseResultTransformerWithStartWith_Shard()
		{
			using (GetNewServer(8079))
			using (GetNewServer(8078))
			using (var store1 = CreateDocumentStore(8079))
			using (var store2 = CreateDocumentStore(8078))
			using (var shardedDocumentStore = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			{
				{"1", store1},
				{"2", store2},
			})))
			{
				shardedDocumentStore.Initialize();
				shardedDocumentStore.ShardStrategy.ModifyDocumentId = (convention, id, documentId) => documentId + "-" + id; // postfix notation for shards

				new FooCapitalNameTransformer().Execute(shardedDocumentStore); // for shards don't use LoadDocument in a transformer

				using (var session = shardedDocumentStore.OpenSession())
				{
					var foo1 = new Foo()
					{
						Id = "foos/1",
						Name = "abc"
					};

					var foo2 = new Foo()
					{
						Id = "foos/2",
						Name = "def"
					};

					session.Store(foo1);
					session.Store(foo2);

					session.SaveChanges();

					var fooBars = session.Advanced.LoadStartingWith<FooCapitalNameTransformer, FooCapitalNameDto>("foos/", configure:
																						x => x.AddQueryParam("input", "testParam")).OrderBy(x => x.Name).ToArray();

					Assert.Equal(2, fooBars.Length);

					Assert.Equal("ABC", fooBars[0].Name);
					Assert.Equal("DEF", fooBars[1].Name);
					Assert.Equal("testParam", fooBars[0].Input);
					Assert.Equal("testParam", fooBars[1].Input);
				}

				using (var asyncSession = shardedDocumentStore.OpenAsyncSession())
				{
					var fooBars = asyncSession.Advanced.LoadStartingWithAsync<FooCapitalNameTransformer, FooCapitalNameDto>("foos/", configure:
																						x => x.AddQueryParam("input", "testParam")).Result.OrderBy(x => x.Name).ToArray();

					Assert.Equal(2, fooBars.Length);

					Assert.Equal("ABC", fooBars[0].Name);
					Assert.Equal("DEF", fooBars[1].Name);
					Assert.Equal("testParam", fooBars[0].Input);
					Assert.Equal("testParam", fooBars[1].Input);
				}
			}
		}

		private static IDocumentStore CreateDocumentStore(int port,  [CallerMemberName] string databaseName = null)
		{
			return new DocumentStore
			{
				Url = string.Format("http://localhost:{0}/", port),
				DefaultDatabase = databaseName,
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.FailImmediately
				}
			};
		}

		private static void DoTest(IDocumentStore store)
		{
			new FooBarTransformer().Execute(store);

			using (var session = store.OpenSession())
			{
				var bar1 = new Bar
				{
					Id = "bars/1",
					Name = "1"
				};

				var bar2 = new Bar
				{
					Id = "bars/2",
					Name = "2"
				};
				var foo1 = new Foo()
				{
					Id = "foos/1",
					Bars = new[] {bar1.Id, bar2.Id}
				};

				var foo2 = new Foo()
				{
					Id = "foos/2",
					Name = "Foo_Two"
				};

				session.Store(bar1);
				session.Store(bar2);
				session.Store(foo1);
				session.Store(foo2);

				session.SaveChanges();

				var fooBars =
					session.Advanced.LoadStartingWith<FooBarTransformer, FooBarDto>("foos/",
					                                                                configure:
						                                                                x => x.AddQueryParam("input", "testParam"))
					       .OrderBy(x => x.Name)
					       .ToArray();

				Assert.Equal(2, fooBars.Length);

				Assert.Null(fooBars[0].Name);
				Assert.Equal(2, fooBars[0].Bars.Length);
				Assert.Equal("1", fooBars[0].Bars[0]);
				Assert.Equal("2", fooBars[0].Bars[1]);
				Assert.Equal("testParam", fooBars[0].Input);

				Assert.Equal("Foo_Two", fooBars[1].Name);
				Assert.Equal(0, fooBars[1].Bars.Length);
				Assert.Equal("testParam", fooBars[1].Input);
			}

			using (var session = store.OpenAsyncSession())
			{
				var fooBars = session.Advanced.LoadStartingWithAsync<FooBarTransformer, FooBarDto>("foos/", configure:
																						x => x.AddQueryParam("input", "testParam")).Result
						   .OrderBy(x => x.Name).ToArray();
					       

				Assert.Equal(2, fooBars.Length);

				Assert.Null(fooBars[0].Name);
				Assert.Equal(2, fooBars[0].Bars.Length);
				Assert.Equal("1", fooBars[0].Bars[0]);
				Assert.Equal("2", fooBars[0].Bars[1]);
				Assert.Equal("testParam", fooBars[0].Input);

				Assert.Equal("Foo_Two", fooBars[1].Name);
				Assert.Equal(0, fooBars[1].Bars.Length);
				Assert.Equal("testParam", fooBars[1].Input);
			}
		}
	}
}