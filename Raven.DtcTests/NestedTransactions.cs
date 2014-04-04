using System;
using System.Diagnostics;
using System.Linq;
using System.Transactions;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class NestedTransactions : RavenTest
	{
	    [Fact]
	    public void ShouldBeASingleTransaction()
	    {
            using (var server = GetNewServer(requestedStorage: "esent"))
	        {
                EnsureDtcIsSupported(server);
                
	            using (var store = new DocumentStore {Url = "http://localhost:8079"})
	            {
	                store.Initialize();

	                using (var outer = new TransactionScope(TransactionScopeOption.Required))
	                {
	                    var id = Guid.NewGuid().ToString();
	                    SaveObject(store, id);
	                    var loaded = LoadObject(store, id);

	                    Assert.NotNull(loaded);

	                    outer.Complete();
	                }
	            }
	        }
	    }


	    [Fact]
		public void ShouldBeASingleTransaction_Embedded()
		{
            using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				store.Initialize();

                EnsureDtcIsSupported(store);

				using (var outer = new TransactionScope(TransactionScopeOption.Required))
				{
					var id = Guid.NewGuid().ToString();
					SaveObject(store, id);
					var loaded = LoadObject(store, id);

					Assert.NotNull(loaded);

					outer.Complete();
				}
			}
		}

		private static void SaveObject(IDocumentStore store, string id)
		{
			using (var session = store.OpenSession())
			{
				session.Store(new SimpleObject(id));
				session.SaveChanges();
			}
		}
		private static SimpleObject LoadObject(IDocumentStore store, string id)
		{
			using (var session = store.OpenSession())
				return session.Load<SimpleObject>(id);
		}


		public class SimpleObject
		{
			public string Id { get; set; }
			public SimpleObject(string id)
			{
				this.Id = id;
			}
		}
	}
}