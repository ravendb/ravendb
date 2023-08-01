namespace Raven.Client.Documents.Queries.Facets
{
    public sealed class FacetAggregationField
    {
        public string Name { get; set; }

        public string DisplayName { get; set; }

        private bool Equals(FacetAggregationField other)
        {
            return Name == other.Name && DisplayName == other.DisplayName;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((FacetAggregationField)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ (DisplayName != null ? DisplayName.GetHashCode() : 0);
            }
        }
    }
}
