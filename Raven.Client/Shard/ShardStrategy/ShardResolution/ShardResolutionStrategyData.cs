using System;

namespace Raven.Client.Shard.ShardStrategy.ShardResolution
{
    public class ShardResolutionStrategyData
    {
        public static ShardResolutionStrategyData BuildFrom(Type type) 
        {
            if (type == null) 
				throw new ArgumentNullException("type");

            return new ShardResolutionStrategyData
            {
                EntityType = type
            };
        }

        public Type EntityType { get; set; }
    }
}
