using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.Bugs.Indexing
{
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