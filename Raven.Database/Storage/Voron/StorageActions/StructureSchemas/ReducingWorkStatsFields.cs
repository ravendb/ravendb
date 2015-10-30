// -----------------------------------------------------------------------
//  <copyright file="ReducingWorkStatsFields.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.StorageActions.StructureSchemas
{
    public enum ReducingWorkStatsFields
    {
        ReduceAttempts,
        ReduceSuccesses,
        ReduceErrors,
        LastReducedEtag,
        LastReducedTimestamp
    }
}
