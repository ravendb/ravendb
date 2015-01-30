// -----------------------------------------------------------------------
//  <copyright file="ReduceResultFields.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.StorageActions.StructureSchemas
{
	public enum ReduceResultFields
	{
		IndexId,
		Level,
		SourceBucket,
		Bucket,
		Timestamp,
		ReduceKey,
		Etag
	}
}