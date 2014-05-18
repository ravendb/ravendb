using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Threading;

namespace Raven.Client.RavenFS.Shard
{
    public class ShardResolutionResult
    {
        public string ShardId { get; set; }
        public string NewFileName { get; set; }
    }
    public class DefaultShardResolutionStrategy : IShardResolutionStrategy
    {
        private readonly ShardStrategy shardStrategy;

        protected delegate string ShardFieldForQueryingFunc(Type entityType);

        private int counter;
        protected readonly List<string> ShardIds;

        public DefaultShardResolutionStrategy(IEnumerable<string> shardIds, ShardStrategy shardStrategy)
        {
            this.shardStrategy = shardStrategy;
            ShardIds = new List<string>(shardIds);
            if (ShardIds.Count == 0)
                throw new ArgumentException("shardIds must have at least one value", "shardIds");
        }

        public virtual ShardResolutionResult GetShardIdForUpload(string filename, RavenJObject metadata)
        {
            var shardId = (
                             from kvp in ShardIds
                             where filename.StartsWith(kvp + shardStrategy.Conventions.IdentityPartsSeparator,
                                                     StringComparison.InvariantCultureIgnoreCase)
                             select kvp
                         ).FirstOrDefault();

            if (shardId == null)
            {
                shardId = GenerateShardIdFor(filename, metadata);
                filename = shardStrategy.ModifyFileName(shardStrategy.Conventions, shardId, filename);
            }
            return new ShardResolutionResult
                {
                    ShardId = shardId,
                    NewFileName = filename
                };
        }

        public virtual string GetShardIdFromFileName(string filename)
        {
	        if (filename.StartsWith("/"))
		        filename = filename.TrimStart(new[] {'/'});
            var start = filename.IndexOf(shardStrategy.Conventions.IdentityPartsSeparator, StringComparison.OrdinalIgnoreCase);
            if (start == -1)
                throw new InvalidDataException("file name does not have the required file name");

            var maybeShardId = filename.Substring(0, start);

            if (ShardIds.Any(x => string.Equals(maybeShardId, x, StringComparison.OrdinalIgnoreCase)))
                return maybeShardId;

            throw new InvalidDataException("could not find a shard with the id: " + maybeShardId);
        }

        /// <summary>
        ///  Generate a shard id for the specified entity
        ///  </summary>
        public virtual string GenerateShardIdFor(string filename, RavenJObject metadata)
        {
            var current = Interlocked.Increment(ref counter);
            return ShardIds[current % ShardIds.Count];
        }

        /// <summary>
        ///  Selects the shard ids appropriate for the specified data.
        ///  </summary><returns>Return a list of shards ids that will be search. Returning null means search all shards.</returns>
        public virtual IList<string> PotentialShardsFor(ShardRequestData requestData)
        {
            if (requestData.Keys.Count == 0) // we are only optimized for keys
                return null;

            // we are looking for search by key, let us see if we can narrow it down by using the 
            // embedded shard id.
            var list = new List<string>();
            foreach (var key in requestData.Keys)
            {
                var start = key.IndexOf(shardStrategy.Conventions.IdentityPartsSeparator, StringComparison.OrdinalIgnoreCase);
                if (start == -1)
                    return null; // if we couldn't figure it out, select from all

                var maybeShardId = key.Substring(0, start);

                if (ShardIds.Any(x => string.Equals(maybeShardId, x, StringComparison.OrdinalIgnoreCase)))
                    list.Add(maybeShardId);
                else
                    return null; // we couldn't find it there, select from all

            }
            return list.ToArray();
        }
    }
}