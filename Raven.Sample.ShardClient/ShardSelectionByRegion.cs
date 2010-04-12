using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Shard;
using Raven.Client;
using Raven.Client.Shard.ShardStrategy.ShardSelection;

namespace Raven.Sample.ShardClient
{
    public class ShardSelectionByRegion : IShardSelectionStrategy
    {
        public string SelectShardIdForNewObject(object obj)
        {
            var company = obj as Company;
            if (company != null)
            {
                switch (company.Region)
                {
                    case "A":
                        return "Shard1";
                    case "B":
                        return "Shard2";
                }
            }

            return null;
        }
    }
}
