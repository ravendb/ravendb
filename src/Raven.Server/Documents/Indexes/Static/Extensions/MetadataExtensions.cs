using System;
using System.Collections.Generic;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static.Extensions
{
    public static class MetadataExtensions
    {
        public static IEnumerable<dynamic> WhereEntityIs(this IEnumerable<dynamic> self, params string[] collections)
        {
            foreach (var document in self)
            {
                var dynamicBlittableJson = document as DynamicBlittableJson;
                if (dynamicBlittableJson == null)
                    continue;

                var collectionName = CollectionName.GetCollectionName(dynamicBlittableJson);
                if (collections.Contains(collectionName, StringComparer.OrdinalIgnoreCase) == false)
                    continue;

                yield return document;
            }
        }

        public static dynamic IfEntityIs(this object self, string collection)
        {
            var document = self as DynamicBlittableJson;
            if (document == null)
                return DynamicNullObject.Null;

            var collectionName = CollectionName.GetCollectionName(document);
            if (string.Equals(collection, collectionName, StringComparison.OrdinalIgnoreCase) == false)
                return DynamicNullObject.Null;

            return document;
        }
    }
}