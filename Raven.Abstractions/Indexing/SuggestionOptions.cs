using Raven.Abstractions.Data;

namespace Raven.Abstractions.Indexing
{
	public class SuggestionOptions
	{
		/// <summary>
		/// Gets or sets the field that supports the suggestion feature.
		/// </summary>
		/// <value>The field.</value>
		public string Field { get; set; }

		/// <summary>
		/// Gets or sets the string distance algorithm.
		/// </summary>
		/// <value>The distance.</value>
		public StringDistanceTypes Distance { get; set; }

		/// <summary>
		/// Gets or sets the accuracy.
		/// </summary>
		/// <value>The accuracy.</value>
		public float Accuracy { get; set; }
	}
}