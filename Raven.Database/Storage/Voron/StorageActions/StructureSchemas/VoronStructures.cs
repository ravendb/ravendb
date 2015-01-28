// -----------------------------------------------------------------------
//  <copyright file="VoronStructures.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Voron;

namespace Raven.Database.Storage.Voron.StorageActions.StructureSchemas
{
	public class VoronStructures
	{
		public readonly StructureSchema<IndexingWorkStatsFields> IndexingWorkStatsSchema = new StructureSchema<IndexingWorkStatsFields>()
			.Add<int>(IndexingWorkStatsFields.IndexId)
			.Add<int>(IndexingWorkStatsFields.IndexingAttempts)
			.Add<int>(IndexingWorkStatsFields.IndexingSuccesses)
			.Add<int>(IndexingWorkStatsFields.IndexingErrors)
			.Add<long>(IndexingWorkStatsFields.LastIndexingTime)
			.Add<long>(IndexingWorkStatsFields.CreatedTimestamp);

		public readonly StructureSchema<ReducingWorkStatsFields> ReducingWorkStatsSchema = new StructureSchema<ReducingWorkStatsFields>()
			.Add<int>(ReducingWorkStatsFields.ReduceAttempts)
			.Add<int>(ReducingWorkStatsFields.ReduceSuccesses)
			.Add<int>(ReducingWorkStatsFields.ReduceErrors)
			.Add<long>(ReducingWorkStatsFields.LastReducedTimestamp)
			.Add<byte[]>(ReducingWorkStatsFields.LastReducedEtag);

		public readonly StructureSchema<LastIndexedStatsFields> LastIndexedStatsSchema = new StructureSchema<LastIndexedStatsFields>()
			.Add<int>(LastIndexedStatsFields.IndexId)
			.Add<long>(LastIndexedStatsFields.LastTimestamp)
			.Add<byte[]>(LastIndexedStatsFields.LastEtag);
	}
}