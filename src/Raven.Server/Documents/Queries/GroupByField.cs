using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Queries
{
    public class GroupByField
    {
        public readonly QueryFieldName Name;

        public readonly bool IsArray;

        public readonly GroupByArrayBehavior GroupByArrayBehavior;

        public GroupByField(QueryFieldName name, bool array)
        {
            Name = name;
            IsArray = array;
            GroupByArrayBehavior = array ? GroupByArrayBehavior.ByIndividualValues : GroupByArrayBehavior.ByContent; // TODO arek RavenDB-8761 - array(Lines[].Product) will set GroupByArrayBehavior.ByContent
        }
    }
}
