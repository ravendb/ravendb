using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.Shard
{
    public class ShardResolutionStrategyData
    {
        public static ShardResolutionStrategyData BuildFrom(Type type) 
        {
            if (type == null) throw new ArgumentNullException("type");

            return new ShardResolutionStrategyData
            {
                EntityType = type
            };
        }

        public Type EntityType { get; set; }
    }
}
