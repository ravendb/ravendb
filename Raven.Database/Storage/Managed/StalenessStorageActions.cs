//-----------------------------------------------------------------------
// <copyright file="StalenessStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualBasic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Exceptions;
using Raven.Database.Impl;
using Raven.Database.Impl.Synchronization;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
	public class StalenessStorageActions : IStalenessStorageActions
	{
		private readonly TableStorage storage;
	    private readonly IListsStorageActions listStorageActions;

	    public StalenessStorageActions(TableStorage storage, IListsStorageActions listStorageActions)
		{
		    this.storage = storage;
		    this.listStorageActions = listStorageActions;
		}

	    public bool IsIndexStale(int view, DateTime? cutOff, Etag cutoffEtag)
      {
        var indexingStatsReadResult = storage.IndexingStats.Read(view.ToString());
        var lastIndexedEtagsReadResult = storage.LastIndexedEtags.Read(view.ToString());

        if (indexingStatsReadResult == null)
          return false;// index does not exists

        if (IsMapStale(view) || IsReduceStale(view))
        {
          if (cutOff != null)
          {
            var lastIndexedTime = lastIndexedEtagsReadResult.Key.Value<DateTime>("lastTimestamp");
            if (cutOff.Value >= lastIndexedTime)
              return true;

            var lastReducedTime = indexingStatsReadResult.Key.Value<DateTime?>("lastReducedTimestamp");
            if (lastReducedTime != null && cutOff.Value >= lastReducedTime.Value)
              return true;
          }
          else if (cutoffEtag != null)
          {
            var lastIndexedEtag = lastIndexedEtagsReadResult.Key.Value<byte[]>("lastEtag");

            if (Buffers.Compare(lastIndexedEtag, cutoffEtag.ToByteArray()) < 0)
              return true;
          }
          else
          {
            return true;
          }
        }

			var tasksAfterCutoffPoint = storage.Tasks["ByIndexAndTime"].SkipTo(new RavenJObject { { "index", view } });
			if (cutOff != null)
				tasksAfterCutoffPoint = tasksAfterCutoffPoint
					.Where(x => x.Value<DateTime>("time") <= cutOff.Value);
			return tasksAfterCutoffPoint.Any();
		}

		public bool IsReduceStale(int view)
		{
			return storage.ScheduleReductions["ByView"].SkipTo(new RavenJObject
			{
				{ "view", view }
			})
			.TakeWhile(token => view == token.Value<int>("view"))
			.Any();
		}

		public bool IsMapStale(int view)
		{
			var readResult = storage.LastIndexedEtags.Read(view.ToString());

			if (readResult == null)
				return false;

			var lastIndexedEtag = readResult.Key.Value<byte[]>("lastEtag");

			var isStale =
				storage.Documents["ByEtag"]
				.SkipFromEnd(0)
				.Select(doc => doc.Value<byte[]>("etag"))
				.Select(docEtag => Buffers.Compare(docEtag, lastIndexedEtag) > 0)
				.FirstOrDefault();

			return isStale;
		}

		public Tuple<DateTime, Etag> IndexLastUpdatedAt(int view)
		{
			var indexingStatsReadResult = storage.IndexingStats.Read(view.ToString());

			if (indexingStatsReadResult == null)
				throw new IndexDoesNotExistsException("Could not find index named: " + view);

			var lastIndexedEtagReadResult = storage.LastIndexedEtags.Read(view.ToString());

			if (indexingStatsReadResult.Key.Value<object>("lastReducedTimestamp") != null)
			{
				return Tuple.Create(
					indexingStatsReadResult.Key.Value<DateTime>("lastReducedTimestamp"),
					Etag.Parse(indexingStatsReadResult.Key.Value<byte[]>("lastReducedEtag"))
					);
			}

			return Tuple.Create(lastIndexedEtagReadResult.Key.Value<DateTime>("lastTimestamp"),
				Etag.Parse(lastIndexedEtagReadResult.Key.Value<byte[]>("lastEtag")));
		}

		public int GetIndexTouchCount(int view)
		{
			var readResult = storage.IndexingStats.Read(view.ToString());

			if (readResult == null)
				throw new IndexDoesNotExistsException("Could not find index named: " + view);

			return readResult.Key.Value<int>("touches");
		}

	    public Etag GetMostRecentDocumentEtag()
		{
			foreach (var doc in storage.Documents["ByEtag"].SkipFromEnd(0))
			{
				var docEtag = doc.Value<byte[]>("etag");
				return Etag.Parse(docEtag);
			}
			return Etag.Empty;
		}

		public Etag GetMostRecentAttachmentEtag()
		{
			foreach (var doc in storage.Attachments["ByEtag"].SkipFromEnd(0))
			{
				var docEtag = doc.Value<byte[]>("etag");
				return Etag.Parse(docEtag);
			}
			return Etag.Empty;
		}
	}
}