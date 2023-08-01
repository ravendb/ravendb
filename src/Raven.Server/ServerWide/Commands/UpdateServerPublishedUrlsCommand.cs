using System;
using System.Collections.Generic;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class UpdateServerPublishedUrlsCommand : CommandBase
    {
        public const string ClusterUrlsKey = "server-published-urls";
        
        public string NodeTag;
        public string PublicUrl;
        public string PrivateUrl;

        public UpdateServerPublishedUrlsCommand()
        {
        }

        public UpdateServerPublishedUrlsCommand(string nodeTag, string publicUrl, string privateUrl, string raftId) : base(raftId)
        {
            NodeTag = nodeTag;
            PublicUrl = publicUrl;
            PrivateUrl = privateUrl;
        }

        public void Update(ClusterOperationContext context, long index)
        {
            var published = PublishedServerUrls.Read(context);
            var urls = published.Urls;

            if (urls.TryGetValue(NodeTag, out var value))
            {
                if (value.Index >= index)
                    return; // try to update to old value

                if (value.PrivateUrl == PrivateUrl && value.PublicUrl == PublicUrl)
                    return; // same value
            }

            value ??= new UrlInfo();
            value.PrivateUrl = PrivateUrl;
            value.PublicUrl = PublicUrl;
            value.Index = index;

            urls[NodeTag] = value;

            using (Slice.From(context.Allocator, ClusterUrlsKey, out var key))
            using (var updated = context.ReadObject(published.ToJson(), "update-cluster-urls"))
            {
                ClusterStateMachine.UpdateValueForItemsTable(context, index, key, key, updated);
            }
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(PublicUrl)] = PublicUrl;
            json[nameof(PrivateUrl)] = PrivateUrl;
            json[nameof(NodeTag)] = NodeTag;
            return json;
        }
    }

    public sealed class PublishedServerUrls : IDynamicJson
    {
        private static Func<BlittableJsonReaderObject, PublishedServerUrls> _converter = JsonDeserializationBase.GenerateJsonDeserializationRoutine<PublishedServerUrls>();

        [JsonDeserializationStringDictionary(StringComparison.OrdinalIgnoreCase)]
        public Dictionary<string, UrlInfo> Urls; // map node tag to private url

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Urls)] = DynamicJsonValue.Convert(Urls)
            };
        }

        public static PublishedServerUrls Read(ClusterOperationContext context)
        {
            using (Slice.From(context.Allocator, UpdateServerPublishedUrlsCommand.ClusterUrlsKey, out var key))
            {
                var current = ClusterStateMachine.ReadInternal(context, out _, key);
                if (current == null)
                    return new PublishedServerUrls { Urls = new Dictionary<string, UrlInfo>(StringComparer.OrdinalIgnoreCase) };

                return _converter(current);
            }
        }

        public static void Clear(ClusterOperationContext context) => ClusterStateMachine.DeleteItem(context, UpdateServerPublishedUrlsCommand.ClusterUrlsKey);

        public string SelectUrl(string requestedTag, ClusterTopology clusterTopology)
        {
            if (Urls.TryGetValue(requestedTag, out var info))
                return info.PrivateUrl;

            return clusterTopology.GetUrlFromTag(requestedTag);
        }
    }

    public sealed class UrlInfo : IDynamicJson
    {
        public string PublicUrl;
        public string PrivateUrl;
        public long Index;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(PublicUrl)] = PublicUrl,
                [nameof(PrivateUrl)] = PrivateUrl,
                [nameof(Index)] = Index,
            };
        }
    }
}
