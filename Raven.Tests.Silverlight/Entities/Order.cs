namespace Raven.Tests.Silverlight.Entities
{
	using System.Collections.Generic;

	public class Order
	{
		public Order()
		{
			Lines = new List<OrderLine>();
		}

		public string Id { get; set; }
		public string Note { get; set; }
		public DenormalizedReference Customer { get; set; }
		public List<OrderLine> Lines { get; set; }
	}
}