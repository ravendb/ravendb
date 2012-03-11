//-----------------------------------------------------------------------
// <copyright file="ShardResolutionByRegion.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Client.Shard;

namespace Raven.Sample.ShardClient
{
	public class ShardResolutionByRegion : IShardResolutionStrategy
	{
		public string GenerateShardIdFor(object entity)
		{
			var company = entity as Company;
			if (company != null)
			{
				return company.Region;
			}
			return null;
		}

		public string MetadataShardIdFor(object entity)
		{
			// We can select one of the shards to hold the metadata entities like the HiLo document for all of the shards:
			return "Asia";

			// Or we can store the metadata on each of the shads itself, so each shards will have its own HiLo document:
			var company = entity as Company;
			if (company != null)
			{
				return company.Region;
			}
			return null;
		}

		public IList<string> PotentialShardsFor(ShardRequestData requestData)
		{
			if (requestData.EntityType == typeof(Company))
			{
				// You can try to limit the potential shards based on the query
			}
			return null;
		}
	}
}