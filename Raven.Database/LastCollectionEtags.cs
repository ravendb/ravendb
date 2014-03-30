using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using Amazon.SimpleEmail.Model;
using ICSharpCode.NRefactory.CSharp.Refactoring.ExtractMethod;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database
{
    public class LastCollectionEtags
    {
        public ITransactionalStorage TransactionalStorage { get; private set; }
        private readonly ConcurrentDictionary<string, Etag> lastCollectionEtags = new ConcurrentDictionary<string, Etag>();

        public LastCollectionEtags(ITransactionalStorage storage)
        {
            this.TransactionalStorage = storage;
        }

        public void Initialize()
        {
            TransactionalStorage.Batch(accessor =>
                accessor.Lists.Read("Raven/Collection/Etag", Etag.Empty, null, int.MaxValue)
                   .ForEach(x => lastCollectionEtags[x.Key] = Etag.Parse(x.Data.Value<string>("Etag"))));

            var lastKnownEtag = Etag.Empty;
            if (!lastCollectionEtags.TryGetValue("All", out lastKnownEtag))
                lastKnownEtag = Etag.Empty;
            var lastDatabaseEtag = Etag.Empty;
            TransactionalStorage.Batch(accessor => { lastDatabaseEtag = accessor.Staleness.GetMostRecentDocumentEtag(); });
            SeekMissingEtagsFrom(lastKnownEtag, lastDatabaseEtag);
        }

        private void SeekMissingEtagsFrom(Etag lastKnownEtag, Etag destinationEtag)
        {
            if (lastKnownEtag.CompareTo(destinationEtag) >= 0) return;

            TransactionalStorage.Batch(accessor =>
            {
                lastKnownEtag = UpdatePerCollectionEtags(
                    accessor.Documents.GetDocumentsAfter(lastKnownEtag, 1000));
            });

            if(lastKnownEtag != null)
                SeekMissingEtagsFrom(lastKnownEtag, destinationEtag);
        }


        private void WriteLastEtagsForCollections()
        {
             TransactionalStorage.Batch(accessor => lastCollectionEtags.ForEach(x=> 
                     accessor.Lists.Set("Raven/Collection/Etag", x.Key, RavenJObject.FromObject(new { Etag = x.Value }), UuidType.Documents)));
        }

        public Etag ReadLastETagForCollection(string collectionName)
        {
            Etag value = Etag.Empty;
            TransactionalStorage.Batch(accessor =>
            {
                var dbvalue = accessor.Lists.Read("Raven/Collection/Etag", collectionName);
                if (dbvalue != null) value = Etag.Parse(dbvalue.Data.Value<string>("Etag"));
            });
            return value;
        }

        public Etag OptimizeCutoffForIndex(AbstractViewGenerator viewGenerator, Etag cutoffEtag)
        {
            if (cutoffEtag != null) return cutoffEtag;
            if (viewGenerator.ReduceDefinition == null && viewGenerator.ForEntityNames.Count > 0)
            {
                var etags = viewGenerator.ForEntityNames.Select(GetLastEtagForCollection)
                                        .Where(x=> x != null);
                if (etags.Any())
                    return etags.Max();
            }
            return null;
        }

        public Etag UpdatePerCollectionEtags(IEnumerable<JsonDocument> documents)
        {
            if (!documents.Any()) return null;

            var collections = documents.GroupBy(x => x.Metadata[Constants.RavenEntityName])
                .Where(x=>x.Key != null)
                .Select(x => new { Etag = x.Max(y => y.Etag), CollectionName = x.Key.ToString()})
                .ToArray();
             
            foreach (var collection in collections)
                UpdateLastEtagForCollection(collection.CollectionName, collection.Etag);

            var maximumEtag = documents.Max(x => x.Etag);
            UpdateLastEtagForCollection("All", maximumEtag);
            return maximumEtag;
        }

        private void UpdateLastEtagForCollection(string collectionName, Etag etag)
        {
            lastCollectionEtags.AddOrUpdate(collectionName, etag,
                (v, oldEtag) => etag.CompareTo(oldEtag) < 0 ? oldEtag : etag);
        }

        public Etag GetLastEtagForCollection(string collectionName)
        {
            Etag result = Etag.Empty;
            if (lastCollectionEtags.TryGetValue(collectionName, out result))
                return result;
            return null;
        }

        public void Flush()
        {
            this.WriteLastEtagsForCollections();
        }
    }
}