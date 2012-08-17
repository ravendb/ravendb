//-----------------------------------------------------------------------
// <copyright file="StalenessStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Database.Exceptions;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
	public class StalenessStorageActions : IStalenessStorageActions
	{
		private readonly TableStorage storage;

		public StalenessStorageActions(TableStorage storage)
		{
			this.storage = storage;
		}

		public bool IsIndexStale(string name, DateTime? cutOff, Guid? cutoffEtag)
		{
			var readResult = storage.IndexingStats.Read(name);

			if (readResult == null)
				return false;// index does not exists


			if (IsMapStale(name) || IsReduceStale(name))
			{
				if (cutOff != null)
				{
					var lastIndexedTime = readResult.Key.Value<DateTime>("lastTimestamp");
					if (cutOff.Value >= lastIndexedTime)
						return true;

					var lastReducedTime = readResult.Key.Value<DateTime?>("lastReducedTimestamp");
					if (lastReducedTime != null && cutOff.Value >= lastReducedTime.Value)
						return true;
				}
				else if (cutoffEtag != null)
				{
					var lastIndexedEtag = readResult.Key.Value<byte[]>("lastEtag");

					if (Buffers.Compare(lastIndexedEtag, cutoffEtag.Value.ToByteArray()) < 0)
						return true;
				}
				else
				{
					return true;
				}
			}
			
			var tasksAfterCutoffPoint = storage.Tasks["ByIndexAndTime"].SkipTo(new RavenJObject{{"index", name}});
			if (cutOff != null)
				tasksAfterCutoffPoint = tasksAfterCutoffPoint
					.Where(x => x.Value<DateTime>("time") <= cutOff.Value);
			return tasksAfterCutoffPoint.Any();
		}

		public bool IsReduceStale(string name)
		{
			return storage.ScheduleReductions["ByView"].SkipTo(new RavenJObject
			{
				{ "view", name }
			})
			.TakeWhile(token => string.Equals(name, token.Value<string>("view"), StringComparison.InvariantCultureIgnoreCase))
			.Any();
		}

		public bool IsMapStale(string name)
		{
			var readResult = storage.IndexingStats.Read(name);

			if (readResult == null)
				return false;// index does not exists

			var lastIndexedEtag = readResult.Key.Value<byte[]>("lastEtag");

			return storage.Documents["ByEtag"].SkipFromEnd(0)
				.Select(doc => doc.Value<byte[]>("etag"))
				.Select(docEtag => Buffers.Compare(docEtag, lastIndexedEtag) > 0)
				.FirstOrDefault();
		}

		public Tuple<DateTime,Guid> IndexLastUpdatedAt(string name)
		{
			var readResult = storage.IndexingStats.Read(name);

			if (readResult == null)
				throw new IndexDoesNotExistsException("Could not find index named: " + name);


			if (readResult.Key.Value<object>("lastReducedTimestamp") != null)
			{
				return Tuple.Create(
					readResult.Key.Value<DateTime>("lastReducedTimestamp"),
					new Guid(readResult.Key.Value<byte[]>("lastReducedEtag"))
					);
			}

			return Tuple.Create(
				readResult.Key.Value<DateTime>("lastTimestamp"),
				new Guid(readResult.Key.Value<byte[]>("lastEtag"))
				);
		}

		public int GetIndexTouchCount(string name)
		{
			var readResult = storage.IndexingStats.Read(name);

			if (readResult == null)
				throw new IndexDoesNotExistsException("Could not find index named: " + name);

			return readResult.Key.Value<int>("touches");
		}

		public Guid GetMostRecentDocumentEtag()
		{
			foreach (var doc in storage.Documents["ByEtag"].SkipFromEnd(0))
			{
				var docEtag = doc.Value<byte[]>("etag");
				return new Guid(docEtag);
			}
			return Guid.Empty;
		}

		public Guid GetMostRecentAttachmentEtag()
		{
			foreach (var doc in storage.Attachments["ByEtag"].SkipFromEnd(0))
			{
				var docEtag = doc.Value<byte[]>("etag");
				return new Guid(docEtag);
			}
			return Guid.Empty;
		}

		public Guid? GetMostRecentReducedEtag(string name)
		{
			var keyWithHighestEqualTo = storage.MappedResults["ByViewAndEtag"]
				.GreatestEqual(new RavenJObject {{"view", name}}, token => token.Value<string>("view") == name);

			if(keyWithHighestEqualTo == null)
				return null;

			return new Guid(keyWithHighestEqualTo.Value<byte[]>("etag"));
		}
	}
}