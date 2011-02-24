using System.Transactions;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class MoreDtcIssues : RemoteClientTest
	{
		public class MyTestClass
		{
			public virtual string Id { get; set; }
			public virtual string SomeText { get; set; }
		}

		[Fact]
		public void CanWriteInTransactionScopeAndReadFromAnotherTransactionScope()
		{
			using(GetNewServer())
			using(var store = new DocumentStore{Url = "http://localhost:8080"}.Initialize())
			{
				var testEntity = new MyTestClass() { SomeText = "Foo" };

				using (var ts = new TransactionScope())
				{
					using (var session = store.OpenSession())
					{
						session.Store(testEntity);
						session.SaveChanges();
					}
					ts.Complete();
				}

				using (var ts = new TransactionScope())
				{
					using (var session = store.OpenSession())
					{
						session.Advanced.AllowNonAuthoritiveInformation = false;
						var testEntityRetrieved = session.Load<MyTestClass>(testEntity.Id);
						Assert.Equal(testEntityRetrieved.SomeText, testEntity.SomeText);
					}
				}
			}
		}

		[Fact]
		public void CanWriteInTransactionScopeAndReadOutsideOfTransactionScope()
		{
			using(GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
			{
				var testEntity = new MyTestClass() {SomeText = "Foo"};

				using (var ts = new TransactionScope())
				{
					using (var session = store.OpenSession())
					{
						session.Store(testEntity);
						session.SaveChanges();
					}
					ts.Complete();
				}

				using (var session = store.OpenSession())
				{
					session.Advanced.AllowNonAuthoritiveInformation = false;
					var testEntityRetrieved = session.Load<MyTestClass>(testEntity.Id);
					Assert.Equal(testEntityRetrieved.SomeText, testEntity.SomeText);
				}
			}
		}
	}
}