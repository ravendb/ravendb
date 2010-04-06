using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using System;
using Raven.Client.Interface;

namespace Raven.Client.Shard
{
	public class ShardedDocumentSession : IDocumentSession
	{
        public event Action<object> Stored;

        public ShardedDocumentSession(IShardSelectionStrategy shardSelectionStrategy, params IDocumentSession[] shardSessions)
		{
            this.shardSelectionStrategy = shardSelectionStrategy;
            this.shardSessions = shardSessions;

            foreach (var shardSession in shardSessions)
            {
                shardSession.Stored += this.Stored;
            }
		}

        IShardSelectionStrategy shardSelectionStrategy = null;
        IDocumentSession[] shardSessions = null;

		public T Load<T>(string id)
		{
            throw new NotImplementedException();
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
            string shardId = shardSelectionStrategy.SelectShardIdForNewObject(entity);
            if (String.IsNullOrEmpty(shardId)) throw new ApplicationException("Can't find single shard to use for entity: " + entity.ToString());

            var shardSession = shardSessions.Where(x => x.StoreIdentifier == shardId).FirstOrDefault();
            if (shardSession == null) throw new ApplicationException("Can't find single shard with identifier: " + shardId);

            action(shardSession);
        }

		public void Store<T>(T entity)
		{
            SingleShardAction(entity, shard => shard.Store(entity));
		}

		public void SaveChanges()
		{
            throw new NotImplementedException();
        }

		public IQueryable<T> Query<T>()
		{
            throw new NotImplementedException();
        }

		public IList<T> GetAll<T>() // NOTE: We probably need to ask the user if they can accept stale results
		{
            throw new NotImplementedException();
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