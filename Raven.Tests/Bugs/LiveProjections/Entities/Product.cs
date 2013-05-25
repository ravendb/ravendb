namespace Raven.Tests.Bugs.LiveProjections.Entities
{
	using System.Collections.Generic;

	public class Product
	{
		public string Id { get; set; }

		public string Name { get; set; }

		public ICollection<ProductSku> Variants { get; set; }
	}
}