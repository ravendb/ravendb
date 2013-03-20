namespace Raven.Tests.Bugs.LiveProjections.Views
{
	using System.Collections.Generic;

	public class ProductDetailsReport
	{
		public string Id { get; set; }

		public string Name { get; set; }

		public IList<ProductVariant> Variants { get; set; }
	}
}