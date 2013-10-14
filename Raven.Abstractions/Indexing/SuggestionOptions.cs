using Raven.Abstractions.Data;

namespace Raven.Abstractions.Indexing
{
	public class SuggestionOptions
	{
		public SuggestionOptions()
		{
			Distance = SuggestionQuery.DefaultDistance;
			Accuracy = SuggestionQuery.DefaultAccuracy;
		}

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

		protected bool Equals(SuggestionOptions other)
		{
			return Distance == other.Distance && Accuracy.Equals(other.Accuracy);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((SuggestionOptions)obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return ((int)Distance * 397) ^ Accuracy.GetHashCode();
			}
		}
	}
}