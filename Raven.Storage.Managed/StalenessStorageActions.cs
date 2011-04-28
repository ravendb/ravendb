//-----------------------------------------------------------------------
// <copyright file="StalenessStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;
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

        public bool IsIndexStale(string name, DateTime? cutOff, string entityName)
        {
            var readResult = storage.IndexingStats.Read(name);

            if (readResult == null)
                return false;// index does not exists


            if (IsMapStale(name) || IsReduceStale(name))
            {
                if (cutOff == null)
                    return true;
                var lastIndexedTime = readResult.Key.Value<DateTime>("lastTimestamp");
                if (cutOff.Value >= lastIndexedTime)
                    return true;
                
                var lastReducedTime = readResult.Key.Value<DateTime?>("lastReducedTimestamp");
                if(lastReducedTime != null && cutOff.Value >= lastReducedTime.Value)
                    return true;
            }

            var tasksAfterCutoffPoint = storage.Tasks["ByIndexAndTime"].SkipTo(new RavenJObject{{"index", name}});
            if (cutOff != null)
                tasksAfterCutoffPoint = tasksAfterCutoffPoint
                    .Where(x => x.Value<DateTime>("time") < cutOff.Value);
            return tasksAfterCutoffPoint.Any();
        }

        public bool IsReduceStale(string name)
        {
			if(name=="SavedInventoryIndex" && new StackTrace().GetFrames().Any(x=>x.GetMethod().DeclaringType == typeof(ReducingExecuter)))
			{
				
			}
            var readResult = storage.IndexingStats.Read(name);

            if (readResult == null)
                return false;// index does not exists

        	var lastReducedEtag = readResult.Key.Value<byte[]>("lastReducedEtag") ?? Guid.Empty.ToByteArray();

            var mostRecentReducedEtag = GetMostRecentReducedEtag(name);
            if (mostRecentReducedEtag == null)
                return true;

            return CompareArrays(mostRecentReducedEtag.Value.ToByteArray(), lastReducedEtag) > 0;
   
        }

        public bool IsMapStale(string name)
        {
            var readResult = storage.IndexingStats.Read(name);

            if (readResult == null)
                return false;// index does not exists

            var lastIndexedEtag = readResult.Key.Value<byte[]>("lastEtag");

            return storage.Documents["ByEtag"].SkipFromEnd(0)
                .Select(doc => doc.Value<byte[]>("etag"))
                .Select(docEtag => CompareArrays(docEtag, lastIndexedEtag) > 0)
                .FirstOrDefault();
        }

        public Tuple<DateTime,Guid> IndexLastUpdatedAt(string name)
        {
            var readResult = storage.IndexingStats.Read(name);

            if (readResult == null)
                throw new IndexDoesNotExistsException("Could not find index named: " + name);

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

        public Guid? GetMostRecentReducedEtag(string name)
        {
            using(var enumerable = storage.MappedResults["ByViewAndEtag"]
				.SkipToAndThenBack(new RavenJObject{{"view", name}})
				.GetEnumerator())
            {
				if (enumerable.MoveNext() == false)
					return null;
				// did we find the last item on the view?
				if (enumerable.Current.Value<string>("view") == name)
					return new Guid(enumerable.Current.Value<byte[]>("etag"));

				// maybe we are at another view?
				if (enumerable.MoveNext() == false)
					return null;

				//could't find the name in the table 
				if (enumerable.Current.Value<string>("view") != name)
					return null;

				return new Guid(enumerable.Current.Value<byte[]>("etag"));
            }
        }

        private static int CompareArrays(byte[] docEtagBinary, byte[] indexEtagBinary)
        {
            for (int i = 0; i < docEtagBinary.Length; i++)
            {
                if (docEtagBinary[i].CompareTo(indexEtagBinary[i]) != 0)
                {
                    return docEtagBinary[i].CompareTo(indexEtagBinary[i]);
                }
            }
            return 0;
        }
    }
}