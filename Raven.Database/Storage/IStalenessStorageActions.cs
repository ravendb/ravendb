//-----------------------------------------------------------------------
// <copyright file="IStalenessStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;

namespace Raven.Database.Storage
{
	public interface IStalenessStorageActions
	{
		bool IsIndexStale(string name, DateTime? cutOff, Etag cutoffEtag);

		bool IsReduceStale(string name);
		bool IsMapStale(string name);

		Tuple<DateTime, Etag> IndexLastUpdatedAt(string name);
		Etag GetMostRecentDocumentEtag();
		Etag GetMostRecentAttachmentEtag();
		int GetIndexTouchCount(string indexName);
	}
}