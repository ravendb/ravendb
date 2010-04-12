using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.Shard
{
    public interface IShardAccessStrategy
    {
        IList<T> Apply<T>(
			IList<IDocumentSession> shardSessions, 
			Func<IDocumentSession, IList<T>> operation
			);
    }
}
