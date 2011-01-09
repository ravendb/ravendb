using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class TransactionIndexByMrn : LocalClientTest
	{
		[Fact]
		public void CanCreateIndex()
		{
			using (var store = NewDocumentStore())
			{
				IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(Transaction_ByMrn))), store);
			}
		}
	}


	public class TransactionIndexByMrnRemote : RemoteClientTest
	{
		[Fact]
		public void CanCreateIndex()
		{
			using(GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize())
			{
				IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(Transaction_ByMrn))), store);
			}
		}
	}

	public class Transaction
	{
		public string MRN { get; set; }
	}

	public class Transaction_ByMrn : AbstractIndexCreationTask<Transaction>
	{
		public Transaction_ByMrn()
		{
			Map = transactions => from transaction in transactions
								  select new
								  {
									  MRN =
										  transaction.MRN
								  };
		}
	}
}