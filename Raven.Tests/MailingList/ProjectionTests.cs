// -----------------------------------------------------------------------
//  <copyright file="ProjectionTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Listeners;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class ProjectionTests : IDisposable
	{
		protected DateTimeOffset Now { get; private set; }
		protected EmbeddableDocumentStore DocumentStore { get; private set; }
		protected IDocumentSession Session { get; private set; }

		public ProjectionTests()
		{
			Now = DateTimeOffset.Now;

			DocumentStore = new EmbeddableDocumentStore { RunInMemory = true };
			DocumentStore.RegisterListener(new NoStaleQueriesAllowed());
			DocumentStore.Initialize();
			Session = DocumentStore.OpenSession();

			Setup();
		}

		private void Setup()
		{
			var list = new List<Foo>()
			{
				new Foo {Data = 1},
				new Foo {Data = 2},
				new Foo {Data = 3},
				new Foo {Data = 4},
			};

			list.ForEach(foo => Session.Store(foo));
			Session.SaveChanges();
		}

		public void Dispose()
		{
			Session.Dispose();
			DocumentStore.Dispose();
		}

		//This works as expected
		[Fact]
		public void ActuallyGetData()
		{
			var foos =
				Session
					.Query<Foo>()
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
			var foos =
				Session
					.Query<Foo>()
					.Where(foo => foo.Data > 1)
					.Select(foo => new FooWithId
					{
						Id = foo.Id,
						Data = foo.Data
					})
					.ToList();

			Xunit.Assert.NotNull(foos[0].Id);
		}

		//Fails
		[Fact]
		public void ShouldBeAbleToProjectIdOntoAnotherName()
		{
			var foos =
				Session
					.Query<Foo>()
					.Customize(x=>x.WaitForNonStaleResults())
					.Where(foo => foo.Data > 1)
					.Select(foo => new FooWithFooId
					{
						FooId = foo.Id,
						Data = foo.Data
					})
					.ToList();

			Xunit.Assert.NotNull(foos[0].FooId);
		}

		[Fact]
		public void ShouldBeAbleToProjectIdOntoAnotherName_AndAnotherFieldNamedIdShouldNotBeAffected()
		{
			var foos =
				Session.Query<Foo>()
				.Customize(x=>x.WaitForNonStaleResults())
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