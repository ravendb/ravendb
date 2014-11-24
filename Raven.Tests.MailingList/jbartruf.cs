using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class jbartruf : RavenTest
	{
		public class TestDoc
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void LoadExistingDocAsync_Test()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var testDoc1 = new TestDoc
				{
					Name = "Jacob"
				};
				using (var session = store.OpenAsyncSession())
				{
					session.StoreAsync(testDoc1).Wait();
					session.SaveChangesAsync().Wait();

					var existingDoc = session.LoadAsync<TestDoc>(testDoc1.Id).Result;
					Assert.Equal(existingDoc.Name, "Jacob");
				}
			}
		}

		[Fact]
		public  void LoadNonExistingDocAsync_Test()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					var nonExistingDoc = session.LoadAsync<TestDoc>(54).Result;
					Assert.Null(nonExistingDoc);
				}
			}
		}

		[Fact]
		public void LoadExistingDocAsync_Test_embedded()
		{
			using (var store = NewDocumentStore())
			{
				var testDoc1 = new TestDoc
				{
					Name = "Jacob"
				};
				using (var session = store.OpenAsyncSession())
				{
					session.StoreAsync(testDoc1).Wait();
					session.SaveChangesAsync().Wait();

					var existingDoc = session.LoadAsync<TestDoc>(testDoc1.Id).Result;
					Assert.Equal(existingDoc.Name, "Jacob");
				}
			}
		}

		[Fact]
		public void LoadNonExistingDocAsync_Test_embedded()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					var nonExistingDoc = session.LoadAsync<TestDoc>(54).Result;
					Assert.Null(nonExistingDoc);
				}
			}
		}
	}
}