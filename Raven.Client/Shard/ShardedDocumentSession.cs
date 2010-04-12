using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Shard.ShardStrategy;
using Raven.Client.Shard.ShardStrategy.ShardResolution;
using System;

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
                shardSession.Stored += Stored;
            }
		}

		private readonly IShardStrategy shardStrategy;
		private readonly IDocumentSession[] shardSessions;

		public T Load<T>(string id)
		{
            var shardIds = shardStrategy.ShardResolutionStrategy.SelectShardIdsFromData(ShardResolutionStrategyData.BuildFrom(typeof(T))) ?? new string[] { };

            IDocumentSession[] shardsToUse = shardSessions.Where(x => shardIds.Contains(x.StoreIdentifier)).ToArray();

            //default to all sessions if none found to use
            if (shardIds.Count == 0)
                shardsToUse = shardSessions;

            //if we can narrow down to single shard, explicitly call it
            if (shardsToUse.Length == 1)
            {
                return shardsToUse[0].Load<T>(id);
            }
			var results = shardStrategy.ShardAccessStrategy.Apply(shardsToUse, x => new[] { x.Load<T>(id) });

			return results
				.Where(x => ReferenceEquals(null, x) == false)
				.FirstOrDefault();
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
            if (String.IsNullOrEmpty(shardId)) throw new ApplicationException("Can't find single shard to use for entity: " + entity);

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
                .Apply(shardSessions, x => x.Query<T>().ToList())
                .AsQueryable()
            ;
        }

		public IList<T> GetAll<T>() 
		{
            return shardStrategy.ShardAccessStrategy.Apply(shardSessions, x => x.GetAll<T>());
        }

		public string StoreIdentifier
		{
			get
			{
				return "ShardedSession";
			}
		}

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