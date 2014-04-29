using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace Raven.Client.RavenFS.Shard
{
	/// <summary>
	/// Implementers of this interface provide a way to decide which shards will be queried
	/// for a specified operation
	/// </summary>
	public interface IShardResolutionStrategy
	{
        /// <summary>
        /// Find in which node to put the file
        /// </summary>
        ShardResolutionResult GetShardIdForUpload(string filename, RavenJObject metadata);

        /// <summary>
        /// Find in which node to the file is in
        /// </summary>
        string GetShardIdFromFileName(string filename);

        
		/// <summary>
		///  Generate a shard id for the specified entity
		///  </summary>
        string GenerateShardIdFor(string filename, RavenJObject metadata);

        
		/// <summary>
		///  Selects the shard ids appropriate for the specified data.
		///  </summary><returns>Return a list of shards ids that will be search. Returning null means search all shards.</returns>
		IList<string> PotentialShardsFor(ShardRequestData requestData);
	}
}
