using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Sharding
{
    internal abstract class PrefixedShardingCommand : RavenCommand, IRaftCommand
    {
        private readonly DocumentConventions _conventions;
        private readonly PrefixedShardingSetting _setting;

        protected abstract PrefixedCommandType CommandType { get; }

        protected PrefixedShardingCommand(DocumentConventions conventions, PrefixedShardingSetting setting)
        {
            _conventions = conventions;
            _setting = setting;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/sharding/prefixed";

            return new HttpRequestMessage
            {
                Method = CommandType switch
                {
                    PrefixedCommandType.Add => HttpMethod.Put,
                    PrefixedCommandType.Delete => HttpMethod.Delete,
                    PrefixedCommandType.Update => HttpMethod.Post,
                    _ => throw new ArgumentOutOfRangeException()
                },
                Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream,
                    DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_setting, ctx)).ConfigureAwait(false), _conventions)
            };
        }

        public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
    }

    internal enum PrefixedCommandType
    {
        Add = 1,
        Delete = 2,
        Update = 3
    }
}
