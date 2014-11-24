using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Document
{
	public class DocumentIdTests : RavenTest
	{
		[Fact]
		public void WithSynchronousApiIdsAreGeneratedOnStore()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			using (var session = store.OpenSession())
			{
				var obj = new TestObject { Name = "Test object" };
				session.Store(obj);
				Assert.NotNull(obj.Id);
			}
		}

		[Fact]
		public void WithAsynchronousApiIdsAreGeneratedOnStoreAsync()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			using (var session = store.OpenAsyncSession())
			{
				var obj = new TestObject { Name = "Test object" };
				session.StoreAsync(obj).Wait();
				Assert.NotNull(obj.Id);
			}
		}

		[Fact]
		public void WithAsynchronousAdvancedStoreApiIdsAreGeneratedOnSaveChanges()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			using (var session = store.OpenAsyncSession())
			{
				var obj = new TestObject { Name = "Test object" };
				session.Advanced.Store(obj);
				Assert.Null(obj.Id);

				session.SaveChangesAsync().Wait();
				Assert.NotNull(obj.Id);

			}
		}

		private class TestObject
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
	}
}
