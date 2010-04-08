using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using System;
using Raven.Client.Shard;

namespace Raven.Client.Shard
{
	public class ShardedDocumentSession : IDocumentSession
	{
        public event Action<object> Stored;

        public ShardedDocumentSession(IShardStrategy shardStrategy, params IDocumentSession[] shardSessions)
		{
            this.shardStrategy = shardStrategy;
            this.shardSessions = shardSessions;

            foreach (var shardSession in shardSessions)
            {
                shardSession.Stored += this.Stored;
            }
		}

        IShardStrategy shardStrategy = null;
        IDocumentSession[] shardSessions = null;

		public T Load<T>(string id)
		{
            var shardIds = shardStrategy.ShardResolutionStrategy.SelectShardIdsFromData(ShardResolutionStrategyData.BuildFrom(typeof(T))) ?? new string[] { };

            IDocumentSession[] shardsToUse = shardSessions.Where(x => shardIds.Contains(x.StoreIdentifier)).ToArray();

            //default to all sessions if none found to use
            if (shardIds == null || shardIds.Count == 0)
                shardsToUse = shardSessions;

            //if we can narrow down to single shard, explicitly call it
            if (shardsToUse.Length == 1)
            {
                return shardsToUse[0].Load<T>(id);
            }
            else //otherwise use access strategy to access all of them and return first one
            {
                var results = shardStrategy.ShardAccessStrategy.Apply<T>(shardsToUse, x =>
                {
                    var result = x.Load<T>(id);

                    if (result == null)
                        return new T[] { };
                    else
                        return new[] { result };
                });

                if (results == null || results.Count == 0)
                    return default(T);
                else
                    return results[0];
            }
		}

        private IDocumentSession GetSingleShardSession(string shardId)
        {
            var shardSession = shardSessions.Where(x => x.StoreIdentifier == shardId).FirstOrDefault();
            if (shardSession == null) throw new ApplicationException("Can't find single shard with identifier: " + shardId);
            return shardSession;
        }

        public void StoreAll<T>(IEnumerable<T> entities)
        {
            foreach (var entity in entities)
            {
                Store(entity);
            }
        }

        private void SingleShardAction<T>(T entity, Action<IDocumentSession> action)
        {
            string shardId = shardStrategy.ShardSelectionStrategy.SelectShardIdForNewObject(entity);
            if (String.IsNullOrEmpty(shardId)) throw new ApplicationException("Can't find single shard to use for entity: " + entity.ToString());

            var shardSession = GetSingleShardSession(shardId);

            action(shardSession);
        }

		public void Store<T>(T entity)
		{
            SingleShardAction(entity, shard => shard.Store(entity));
		}

		public void SaveChanges()
		{
            //I don't really understand what the point of this is, given that store sends
            //info to the server and this resends it.. wouldn't that duplicate it?
            throw new NotImplementedException();
        }

        public IQueryable<T> Query<T>()
		{
            //probably need an expression as a parm that can be passed through to each session for this to be useful
            return 
                shardStrategy
                .ShardAccessStrategy
                .Apply<T>(shardSessions, x => x.Query<T>().ToList())
                .AsQueryable()
            ;
        }

		public IList<T> GetAll<T>() 
		{
            return shardStrategy.ShardAccessStrategy.Apply<T>(shardSessions, x => x.GetAll<T>());
        }

        public string StoreIdentifier { get { return "ShardedSession"; } }

        #region IDisposable Members

        public void Dispose()
        {
            foreach (var shardSession in shardSessions)
                shardSession.Dispose();

            //dereference all event listeners
            Stored = null;
        }

        #endregion
 
    }
}