using Raven.Abstractions.Data;

namespace Raven.Abstractions.Indexing
{
	public class SuggestionOptions
	{
		/// <summary>
		/// Gets or sets the string distance algorithm.
		/// </summary>
		/// <value>The distance. The default value is StringDistanceTypes.Levenshtein.</value>
		public StringDistanceTypes Distance { get; set; }

		/// <summary>
		/// Gets or sets the accuracy.
		/// </summary>
		/// <value>The accuracy. The default value is 0.5f.</value>
		public float Accuracy { get; set; }
	}
}