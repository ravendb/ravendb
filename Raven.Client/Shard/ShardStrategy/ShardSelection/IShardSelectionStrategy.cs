using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.Shard
{
    public interface IShardSelectionStrategy
    {
        string SelectShardIdForNewObject(object obj);
    }
}
