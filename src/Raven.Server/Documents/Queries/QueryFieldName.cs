using Raven.Client;

namespace Raven.Server.Documents.Queries
{
    public class QueryFieldName
    {
        public static readonly QueryFieldName Empty = new QueryFieldName(string.Empty, false);
        public static readonly QueryFieldName DocumentId = new QueryFieldName(Constants.Documents.Indexing.Fields.DocumentIdFieldName, false);
        public static readonly QueryFieldName Count = new QueryFieldName(Constants.Documents.Indexing.Fields.CountFieldName, false);
        
        public QueryFieldName(string name, bool isQuoted)
        {
            Value = name;
            IsQuoted = isQuoted;
        }
        
        public static implicit operator string(QueryFieldName self)
        {
            return self?.Value;
        }

        public readonly string Value;

        public readonly bool IsQuoted;

        protected bool Equals(QueryFieldName other)
        {
            return string.Equals(Value, other.Value) && IsQuoted == other.IsQuoted;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != this.GetType())
                return false;
            return Equals((QueryFieldName)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Value != null ? Value.GetHashCode() : 0) * 397) ^ IsQuoted.GetHashCode();
            }
        }

        public override string ToString()
        {
            return Value;
        }
    }
}
