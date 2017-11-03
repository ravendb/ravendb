using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Queries
{
    public class GroupByField
    {
        public readonly QueryFieldName Name;

        public readonly GroupByArrayBehavior GroupByArrayBehavior;

        public readonly string Alias;

        public GroupByField(QueryFieldName name, GroupByArrayBehavior byArrayBehavior, string alias)
        {
            Name = name;
            GroupByArrayBehavior = byArrayBehavior;
            Alias = alias;
        }
    }
}
