//-----------------------------------------------------------------------
// <copyright file="OverwriteDocuments.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Exceptions;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class OverwriteDocuments : IDisposable
	{
		private readonly IDocumentStore documentStore;

		public OverwriteDocuments()
		{
			documentStore = new EmbeddableDocumentStore
			                	{
			                		Configuration =
			                			{
			                				DataDirectory = "HiLoData"
			                			}
			                	}.Initialize();

			documentStore.DatabaseCommands.PutIndex("Foo/Something", new IndexDefinitionBuilder<Foo>
			{
				Map = docs => from doc in docs select new {doc.Something}
			});
		}

		[Fact]
		public void WillThrowWhenOptimisticConcurrencyIsOn()
		{
			using (var session = documentStore.OpenSession()) {
				var foo = new Foo { Id = "foos/1", Something = "something1" };
				session.Store(foo);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				session.Advanced.UseOptimisticConcurrency = true;
				var foo = new Foo { Id = "foos/1", Something = "something1" };
				session.Store(foo);
				Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
			}
		}

		[Fact]
		public void WillOverwriteDocWhenOptimisticConcurrencyIsOff()
		{
			using (var session = documentStore.OpenSession())
			{
				var foo = new Foo { Id = "foos/1", Something = "something1" };
				session.Store(foo);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenSession())
			{
				var foo = new Foo { Id = "foos/1", Something = "something1" };
				session.Store(foo);
				session.SaveChanges();
			}
		}

		private class Foo
		{
			public string Id { get; set; }
			public string Something { get; set; }
		}

		public void Dispose()
		{
			documentStore.Dispose();
			IOExtensions.DeleteDirectory("HiLoData");
		}
	}
}
