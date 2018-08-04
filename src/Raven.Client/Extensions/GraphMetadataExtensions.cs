using System;
using System.Collections.Generic;
using Raven.Client.Documents.Graph;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Extensions
{
    public static class GraphMetadataExtensions
    {
        public static void WriteEdgeData(this BlittableJsonReaderObject metadata, string edgeType, EdgeInfo edgeInfo)
        {
            if (!metadata.TryGet(Constants.Documents.Metadata.Edges, out BlittableJsonReaderArray edgesData))
            {
                throw new InvalidOperationException("Cannot write edge data when @edges is not in @metadata");
            }

            var index = edgesData.BinarySearch(edgeType, StringComparison.InvariantCultureIgnoreCase);
            if (index >= 0)
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();
                ((BlittableJsonReaderObject)edgesData[index]).GetPropertyByIndex(0,ref prop);

                var array = (BlittableJsonReaderArray)prop.Value;
                if(array.Modifications == null)
                    array.Modifications = new DynamicJsonArray();

                array.Modifications.Add(edgeInfo.ToJson());
            }
            else
            {
                if(edgesData.Modifications == null)
                    edgesData.Modifications = new DynamicJsonArray();

                edgesData.Modifications.Add(new DynamicJsonValue
                {
                    [edgeType] = new DynamicJsonArray(new []{ edgeInfo })
                });
            }
        }


        public static IEnumerable<(string edgeType,EdgeInfo edge)> ReadEdgeData(this BlittableJsonReaderObject metadata)
        {
            if (!metadata.TryGet(Constants.Documents.Metadata.Edges, out BlittableJsonReaderArray edgesData))
            {
                yield break;
            }

            foreach (BlittableJsonReaderObject edgeType in edgesData)            
            {
                if(edgeType.Count == 0) //not sure, perhaps error here?
                    continue;

                var prop = new BlittableJsonReaderObject.PropertyDetails();
                edgeType.GetPropertyByIndex(0,ref prop);

                foreach (BlittableJsonReaderObject edgeInfo in (BlittableJsonReaderArray)prop.Value)
                {
                    yield return (prop.Name, JsonDeserializationClient.EdgeInfo(edgeInfo));
                }
                
            }
        }

        public static IEnumerable<EdgeInfo> ReadEdgeData(this BlittableJsonReaderObject metadata,string edgeType)
        {
            if (!metadata.TryGet(Constants.Documents.Metadata.Edges, out BlittableJsonReaderArray edgesData))
            {
                yield break;
            }

            var index = edgesData.BinarySearch(edgeType, StringComparison.InvariantCultureIgnoreCase);
            if(index < 0)
                yield break;

            var prop = new BlittableJsonReaderObject.PropertyDetails();
            ((BlittableJsonReaderObject)edgesData[index]).GetPropertyByIndex(0,ref prop);

            foreach (BlittableJsonReaderObject edgeInfo in (BlittableJsonReaderArray)prop.Value)
            {
                yield return JsonDeserializationClient.EdgeInfo(edgeInfo);
            }
        }
    }
}
