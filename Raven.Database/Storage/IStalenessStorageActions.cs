//-----------------------------------------------------------------------
// <copyright file="IStalenessStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Database.Storage
{
	public interface IStalenessStorageActions
	{
		bool IsIndexStale(int view, DateTime? cutOff, Etag cutoffEtag, IEnumerable<string> collectionNames);

		bool IsReduceStale(int view);
		bool IsMapStale(int view);

		Tuple<DateTime, Etag> IndexLastUpdatedAt(int view);
		Etag GetMostRecentDocumentEtag();
		Etag GetMostRecentAttachmentEtag();
		int GetIndexTouchCount(int view);

    void SetLastEtagForCollection(string collection, Etag etag);
  Etag GetLastEtagForCollection(string collection);
	}
}