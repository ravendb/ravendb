using System;
using System.Collections.Concurrent;
using Raven.Client.Documents.Indexes;

namespace Raven.Client.Documents.Session
{
    internal static class IndexMetadataCache 
    {
        private static readonly ConcurrentDictionary<Type, IndexMetadataCacheItem> IndexMetadataCacheContainer = new();

        internal static IndexMetadataCacheItem GetIndexMetadataCacheItem<TIndexCreator>() where TIndexCreator : AbstractCommonApiForIndexes, new()
        {
            if (IndexMetadataCacheContainer.TryGetValue(typeof(TIndexCreator), out var index) == false)
            {
                index = new IndexMetadataCacheItem(new TIndexCreator());
                IndexMetadataCacheContainer.TryAdd(typeof(TIndexCreator), index);
            }

            return index;
        }
        
        internal class IndexMetadataCacheItem
        {
            public readonly string IndexName;

            public readonly bool IsMapReduce;
            
            public IndexMetadataCacheItem(AbstractCommonApiForIndexes indexCreator)
            {
                IndexName = indexCreator.IndexName;
                IsMapReduce = indexCreator.IsMapReduce;
            }
        }
    }
}
