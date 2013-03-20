using System;
using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Sample.ShardClient
{
	public class InvoicesAmountByDate : AbstractIndexCreationTask<Invoice, InvoicesAmountByDate.ReduceResult>
	{
		public class ReduceResult
		{
			public decimal Amount { get; set; }
			public DateTime IssuedAt { get; set; }
		}

		public InvoicesAmountByDate()
		{
			Map = invoices =>
			      from invoice in invoices
			      select new
			      {
			      	invoice.Amount,
			      	invoice.IssuedAt
			      };

			Reduce = results =>
			         from result in results
			         group result by result.IssuedAt
			         into g
			         select new
			         {
			         	Amount = g.Sum(x => x.Amount),
			         	IssuedAt = g.Key
			         };
		}
	}
}