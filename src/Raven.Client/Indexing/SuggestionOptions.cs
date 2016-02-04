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
        /// String distance algorithm to use. Default algorithm is Levenshtein.
        /// </summary>
        public StringDistanceTypes Distance { get; set; }

        /// <summary>
        /// Suggestion accuracy. If <c>null</c> then default accuracy is used (0.5f).
        /// </summary>
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
