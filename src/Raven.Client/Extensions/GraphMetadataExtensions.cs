using System;
using System.Collections.Generic;
using Raven.Client.Documents.Graph;
using Sparrow.Json;

namespace Raven.Client.Extensions
{
    public static class GraphMetadataExtensions
    {
        private static readonly Func<BlittableJsonReaderObject, List<EdgeInfo>> Deserialize =
            JsonDeserializationBase.GenerateJsonDeserializationRoutine<List<EdgeInfo>>();

        public static List<EdgeInfo> GetEdgeData(this BlittableJsonReaderObject metadata)
        {
            if (!metadata.TryGet("@edges", out BlittableJsonReaderObject edgesData))
            {
                return new List<EdgeInfo>();
            }

            return Deserialize(edgesData);
        }
    }
}
