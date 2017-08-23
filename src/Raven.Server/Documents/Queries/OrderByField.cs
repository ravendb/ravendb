using Raven.Server.Documents.Queries.Parser;

namespace Raven.Server.Documents.Queries
{
    public struct OrderByField
    {
        public OrderByField(string name, OrderByFieldType orderingType, bool ascending, Argument[] arguments)
        {
            Name = name;
            OrderingType = orderingType;
            Ascending = ascending;
            Arguments = arguments;
        }

        public readonly string Name;

        public readonly OrderByFieldType OrderingType;

        public readonly bool Ascending;

        public readonly Argument[] Arguments;

        public struct Argument
        {
            public Argument(string nameOrValue, ValueTokenType type)
            {
                NameOrValue = nameOrValue;
                Type = type;
            }

            public readonly string NameOrValue;

            public readonly ValueTokenType Type;
        }
    }
}
