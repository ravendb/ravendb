namespace Raven.Abstractions.Data
{
	public class FacetValue
	{
		public string Range { get; set; }
		public int Hits { get; set; }

		public override string ToString()
		{
			return string.Format("Range: {0}, Hits: {1}", Range, Hits);
		}
	}
}
