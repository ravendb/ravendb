using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_551 : RavenTest
	{
		[Fact]
	 	public void CanGetErrorOnOptimisticDeleteInTransaction()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				var tx = new TransactionInformation
					{
						Id = Guid.NewGuid()
					};
				Assert.Throws<ConcurrencyException>(() => 
					store.DocumentDatabase.Delete("items/1", Guid.NewGuid(), tx));
			}
		}

		[Fact]
		public void CanGetErrorOnOptimisticDeleteInTransactionWhenModifiedInTransaction()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}
				var tx = new TransactionInformation
				{
					Id = Guid.NewGuid()
				};
				store.DocumentDatabase.Put("items/1", null, new RavenJObject(), new RavenJObject(), tx);
				Assert.Throws<ConcurrencyException>(() =>
					store.DocumentDatabase.Delete("items/1", Guid.NewGuid(), tx));
			}
		}

		public class Item{}
	}
}