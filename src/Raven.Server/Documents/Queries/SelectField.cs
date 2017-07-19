using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Queries
{
    public class SelectField
    {
        public readonly string Name;

        public readonly string Alias;

        public readonly AggregationOperation AggregationOperation;

        public SelectField(string name, string alias = null)
        {
            Name = name;
            Alias = alias;
        }

        public SelectField(string name, string alias, AggregationOperation aggregationOperation)
        {
            Name = name;
            Alias = alias;
            AggregationOperation = aggregationOperation;
        }
    }
}