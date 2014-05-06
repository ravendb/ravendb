// -----------------------------------------------------------------------
//  <copyright file="ProjectionTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Listeners;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class ProjectionTests : RavenTest
	{
		private readonly EmbeddableDocumentStore documentStore;
		private readonly IDocumentSession session;

		public ProjectionTests()
		{
			documentStore = NewDocumentStore(configureStore: store => store.RegisterListener(new NoStaleQueriesAllowed()));
			session = documentStore.OpenSession();

			Setup();
		}

		private void Setup()
		{
			var list = new List<Foo>
			{
				new Foo {Data = 1},
				new Foo {Data = 2},
				new Foo {Data = 3},
				new Foo {Data = 4},
			};

			list.ForEach(foo => session.Store(foo));
			session.SaveChanges();
		}

		//This works as expected
		[Fact]
		public void ActuallyGetData()
		{
			var foos = session.Query<Foo>()
			                  .Where(foo => foo.Data > 1)
			                  .Select(foo => new FooWithId
			                  {
				                  Id = foo.Id,
				                  Data = foo.Data
			                  })
			                  .ToList();

			Assert.True(foos.Count == 3);
		}

		//This works as expected
		[Fact]
		public void ShouldBeAbleToProjectIdOntoAnotherFieldCalledId()
		{
			var foos = session.Query<Foo>()
			                  .Where(foo => foo.Data > 1)
			                  .Select(foo => new FooWithId
			                  {
				                  Id = foo.Id,
				                  Data = foo.Data
			                  })
			                  .ToList();

			Assert.NotNull(foos[0].Id);
		}

		//Fails
		[Fact]
		public void ShouldBeAbleToProjectIdOntoAnotherName()
		{
			var foos = session.Query<Foo>()
			                  .Customize(x => x.WaitForNonStaleResults())
			                  .Where(foo => foo.Data > 1)
			                  .Select(foo => new FooWithFooId
			                  {
				                  FooId = foo.Id,
				                  Data = foo.Data
			                  })
			                  .ToList();

			Assert.NotNull(foos[0].FooId);
		}

		[Fact]
		public void ShouldBeAbleToProjectIdOntoAnotherName_AndAnotherFieldNamedIdShouldNotBeAffected()
		{
			var foos = session.Query<Foo>()
			                  .Customize(x => x.WaitForNonStaleResults())
			                  .Where(foo => foo.Data > 1)
			                  .Select(foo => new FooWithFooIdAndId
			                  {
				                  FooId = foo.Id,
				                  Data2 = foo.Data
			                  })
			                  .ToList();

			Assert.Null(foos[0].Id);
			Assert.NotNull(foos[0].FooId);
		}

		private class Foo
		{
			public string Id { set; get; }
			public int Data { set; get; }
		}

		private class FooWithFooId
		{
			public string FooId { set; get; }
			public int Data { set; get; }
		}

		private class FooWithId
		{
			public string Id { set; get; }
			public int Data { set; get; }
		}

		private class FooWithFooIdAndId
		{
			public string FooId { set; get; }
			public string Id { set; get; }
			public int Data2 { set; get; }
		}

		public class NoStaleQueriesAllowed : IDocumentQueryListener
		{
			public void BeforeQueryExecuted(IDocumentQueryCustomization queryCustomization)
			{
				queryCustomization.WaitForNonStaleResults();
			}
		}
	}
}