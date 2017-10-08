using System.Globalization;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public struct OrderByField
    {
        public OrderByField(QueryFieldName name, OrderByFieldType orderingType, bool ascending, MethodType? method = null, Argument[] arguments = null)
        {
            Method = method;
            Name = name;
            OrderingType = orderingType;
            Ascending = ascending;
            Arguments = arguments;
            AggregationOperation = AggregationOperation.None;
        }

        public readonly QueryFieldName Name;

        public readonly OrderByFieldType OrderingType;

        public readonly bool Ascending;

        public readonly Argument[] Arguments;

        public readonly MethodType? Method;

        public struct Argument
        {
            public Argument(string nameOrValue, ValueTokenType type)
            {
                NameOrValue = nameOrValue;
                Type = type;
            }

            public readonly string NameOrValue;

            public readonly ValueTokenType Type;

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

        public AggregationOperation AggregationOperation;
    }
}
