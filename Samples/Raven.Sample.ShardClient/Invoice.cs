using System;

namespace Raven.Sample.ShardClient
{
	public class Invoice
	{
		public string Id { get; set; }
		public string CompanyId { get; set; }
		public decimal Amount { get; set; }
		public DateTime IssuedAt { get; set; }
	}
}