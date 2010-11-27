using Raven.Client.Shard.ShardStrategy.ShardSelection;

namespace Raven.Sample.ShardClient
{
    public class ShardSelectionByRegion : IShardSelectionStrategy
    {
        public string ShardIdForNewObject(object obj)
        {
            var company = obj as Company;
            if (company != null)
            {
                return company.Region.Replace(" ", "-");
            }

            return null;
        }

        public string ShardIdForExistingObject(object obj)
        {
            return ShardIdForNewObject(obj);
        }
    }
}
