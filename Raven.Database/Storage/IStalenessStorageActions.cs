//-----------------------------------------------------------------------
// <copyright file="IStalenessStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Database.Storage
{
	public interface IStalenessStorageActions
	{
		bool IsIndexStale(string name, DateTime? cutOff, Guid? cutoffEtag);

		bool IsReduceStale(string name);
		bool IsMapStale(string name);

		Tuple<DateTime, Guid> IndexLastUpdatedAt(string name);
		Guid GetMostRecentDocumentEtag();
		Guid GetMostRecentAttachmentEtag();
		Guid? GetMostRecentReducedEtag(string name);
		int GetIndexTouchCount(string indexName);
	}
}