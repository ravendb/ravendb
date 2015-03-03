using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bundles.Replication
{
	public class CollectionSpecific : ReplicationBase
	{
		private DocumentStore store3;
		private DocumentStore store2;
		private DocumentStore store1;

		[Fact]
		public void Replication_from_specified_collections_only_should_work()
		{
			store1 = CreateStore();
			store2 = CreateStore();
			store3 = CreateStore();

			var ids = WriteTracers(store1);

			var aa = store1.DatabaseCommands.Get(ids.Item1);

			SetupReplication(store1.DatabaseCommands, new[] { "C2s" }, store2, store3);

			Assert.True(WaitForDocument(store2.DatabaseCommands, ids.Item2, 1000));
			Assert.False(WaitForDocument(store2.DatabaseCommands, ids.Item1, 1000));

			Assert.True(WaitForDocument(store3.DatabaseCommands, ids.Item2, 1000));
			Assert.False(WaitForDocument(store3.DatabaseCommands, ids.Item1, 1000));

		}

		private Tuple<string, string> WriteTracers(IDocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				var c1 = new C1();
				var c2 = new C2();

				session.Store(c1);
				session.Store(c2);
				session.SaveChanges();

				return Tuple.Create(session.Advanced.GetDocumentId(c1), session.Advanced.GetDocumentId(c2));
			}
		}

		private class C1
		{
		}

		private class C2
		{
		}
	}
}
