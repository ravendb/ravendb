using System.Globalization;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public record struct OrderByField(QueryFieldName Name, OrderByFieldType OrderingType, bool Ascending, MethodType? Method = null, OrderByField.Argument[] Arguments = null, NullsOrderingType NullsOrdering = NullsOrderingType.Implicit)
    {
        public readonly string OrderByName = OrderingType switch
        {
            OrderByFieldType.Long => $"{Name}{Constants.Documents.Indexing.Fields.RangeFieldSuffixLong}",
            OrderByFieldType.Double => $"{Name}{Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble}",
            _ => Name
        };

        public readonly record struct Argument(string NameOrValue, ValueTokenType Type)
        {
            public double GetDouble(BlittableJsonReaderObject parameters)
            {
                double value;
                if (Type != ValueTokenType.Parameter)
                    value = double.Parse(NameOrValue, CultureInfo.InvariantCulture);
                else
                    parameters.TryGet(NameOrValue, out value);

                return value;
            }

            public string GetString(BlittableJsonReaderObject parameters)
            {
                string value;
                if (Type != ValueTokenType.Parameter)
                    value = NameOrValue;
                else
                    parameters.TryGet(NameOrValue, out value);

                return value;
            }
        }

        public AggregationOperation AggregationOperation = AggregationOperation.None;
    }
}
