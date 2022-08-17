using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.TrafficWatch
{
    internal class PutTrafficWatchConfigurationOperation : IServerOperation
    {
        private readonly TrafficWatchConfigurationResult _parameters;

        public PutTrafficWatchConfigurationOperation(TrafficWatchConfigurationResult parameters)
        {
            _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetTrafficWatchConfigurationCommand(_parameters);
        }

        private class SetTrafficWatchConfigurationCommand : RavenCommand
        {
            private readonly TrafficWatchConfigurationResult _parameters;

            public SetTrafficWatchConfigurationCommand(TrafficWatchConfigurationResult parameters)
            {
                _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/traffic-watch/configuration";

                return new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx)).ConfigureAwait(false))
                };
            }
        }
    }
}
