// -----------------------------------------------------------------------
//  <copyright file="Maverix.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Maverix : RavenTest
	{
		[Fact]
		public void SimpleTest1()
		{
			var item = new Simple();

			using (IDocumentStore store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(item);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var stored = session.Load<Simple>(1);
					Assert.NotNull(stored);
					Assert.Equal("simples/1", stored.Id);
				}
			}
		}

		[Fact]
		public void SimpleTest2()
		{
			var item = new Simple();

			using (IDocumentStore store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(item);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var stored = session.Load<Simple>("simples/1");
					Assert.NotNull(stored);
					Assert.Equal("simples/1", stored.Id);
				}
			}
		}

		[Fact]
		public void ComplexTest1()
		{
			var item = new ComplexClass();

			using (IDocumentStore store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(item);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var stored = session.Load<ComplexClass>(1);
					Assert.NotNull(stored);
					Assert.Equal("ComplexClasses/1", stored.Id);
				}
			}
		}

		[Fact]
		public void ComplexTest2()
		{
			var item = new ComplexClass();

			using (IDocumentStore store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(item);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var stored =
						session.Load<ComplexClass>("ComplexClasses/1");
					Assert.NotNull(stored);
					Assert.Equal("ComplexClasses/1", stored.Id);
				}
			}
		}

		private class ComplexClass
		{
			public string Id { get; set; }
		}

		private class Simple
		{
			public string Id { get; set; }
		}
	}
}