//-----------------------------------------------------------------------
// <copyright file="StalenessStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Database.Exceptions;
using Raven.Database.Storage;
using Raven.Json.Linq;
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
            var readResult = storage.IndexingStats.Read(new RavenJObject
            {
                {"index", name}
            });

            if (readResult == null)
                return false;// index does not exists

            var lastIndexedEtag = readResult.Key.Value<byte[]>("lastEtag");
            var lastIndexedTime = readResult.Key.Value<DateTime>("lastTimestamp");

            if (IsStaleByEtag(lastIndexedEtag))
            {
                if (cutOff == null)
                    return true;
                if (cutOff.Value >= lastIndexedTime)
                    return true;
            }

            var keyToSearch = new RavenJObject
            {
                {"index", name},
            };
            var tasksAfterCutoffPoint = storage.Tasks["ByIndexAndTime"].SkipTo(keyToSearch);
            if (cutOff != null)
                tasksAfterCutoffPoint = tasksAfterCutoffPoint
                    .Where(x => x.Value<DateTime>("time") < cutOff.Value);
            return tasksAfterCutoffPoint.Any();
        }

        public Tuple<DateTime,Guid> IndexLastUpdatedAt(string name)
        {
            var readResult = storage.IndexingStats.Read(new RavenJObject
            {
                {"index", name}
            });

            if (readResult == null)
                throw new IndexDoesNotExistsException("Could not find index named: " + name);

            return Tuple.Create(
                readResult.Key.Value<DateTime>("lastTimestamp"),
                new Guid(readResult.Key.Value<byte[]>("lastEtag"))
                );
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

        public Guid GetMostRecentReducedEtag(string name)
        {
            var enumerable = storage.MappedResults["ByViewAndEtag"].SkipToAndThenBack(new RavenJObject{{"view", name}}).GetEnumerator();
            if(enumerable.MoveNext() == false)
                return Guid.Empty;
            // did we find the last item on the view?
            if (enumerable.Current.Value<string>("view") == name)
                return new Guid(enumerable.Current.Value<byte[]>("etag"));

            // maybe we are at another view?
            if (enumerable.MoveNext() == false)
                return Guid.Empty;

            //could't find the name in the table 
            if (enumerable.Current.Value<string>("view") != name)
                return Guid.Empty;

            return new Guid(enumerable.Current.Value<byte[]>("etag"));
        }

        private bool IsStaleByEtag(byte [] lastIndexedEtag)
        {
        	return storage.Documents["ByEtag"].SkipFromEnd(0)
				.Select(doc => doc.Value<byte[]>("etag"))
				.Select(docEtag => CompareArrays(docEtag, lastIndexedEtag) > 0)
				.FirstOrDefault();
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