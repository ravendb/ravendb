using Raven.Client.Documents.Queries.Suggestions;

namespace Raven.Client.Documents.Indexes.Suggestions
{
    public class SuggestionOptions
    {
        public SuggestionOptions()
        {
            Distance = Queries.Suggestions.SuggestionOptions.DefaultDistance;
            Accuracy = Queries.Suggestions.SuggestionOptions.DefaultAccuracy;
        }

        /// <summary>
        /// String distance algorithm to use. Default algorithm is Levenshtein.
        /// </summary>
        public StringDistanceTypes Distance { get; set; }

        /// <summary>
        /// Suggestion accuracy. If <c>null</c> then default accuracy is used (0.5f).
        /// </summary>
        public double Accuracy { get; set; }

        protected bool Equals(SuggestionOptions other)
        {
            return Distance == other.Distance && Accuracy.Equals(other.Accuracy);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
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
