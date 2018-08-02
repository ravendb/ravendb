using System;
using System.Collections.Generic;
using Raven.Client.Documents.Graph;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Extensions
{
    public static class GraphMetadataExtensions
    {
        public static IEnumerable<EdgeInfo> GetEdgeData(this BlittableJsonReaderObject metadata)
        {
            if (!metadata.TryGet(Constants.Documents.Metadata.Edges, out BlittableJsonReaderArray edgesData))
            {
                yield break;
            }

            foreach (BlittableJsonReaderObject e in edgesData)
            {
                yield return JsonDeserializationClient.EdgeInfo(e);
            }
        }
    }
}
