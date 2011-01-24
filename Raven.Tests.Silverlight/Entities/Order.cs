namespace Raven.Tests.Silverlight.Entities
{
	public class Order
	{
		public string Id { get; set; }
		public string Note { get; set; }
		public DenormalizedReference Customer { get; set; }
	}
}