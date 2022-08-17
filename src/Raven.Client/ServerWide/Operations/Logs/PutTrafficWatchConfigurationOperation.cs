using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Size = Sparrow.Size;

namespace Raven.Client.ServerWide.Operations.Logs
{
    internal class PutTrafficWatchConfigurationOperation : IServerOperation
    {
        private readonly Parameters _parameters;

        public class Parameters
        {
            public TrafficWatchMode TrafficWatchMode { get; set; }
            public HashSet<string> Databases { get; set; }
            public HashSet<int> StatusCodes { get; set; }
            public Size MinimumResponseSize { get; set; }
            public Size MinimumRequestSize { get; set; }
            public long MinimumDuration { get; set; }
            public HashSet<string> HttpMethods { get; set; }
            public HashSet<TrafficWatchChangeType> ChangeTypes { get; set; }

            public Parameters()
            { }

            public Parameters(TrafficWatchConfigurationResult currentTrafficWatchConfiguration)
            {
                TrafficWatchMode = currentTrafficWatchConfiguration.TrafficWatchMode;
                Databases = currentTrafficWatchConfiguration.Databases;
                StatusCodes = currentTrafficWatchConfiguration.StatusCodes;
                MinimumResponseSize = currentTrafficWatchConfiguration.MinimumResponseSize;
                MinimumRequestSize = currentTrafficWatchConfiguration.MinimumRequestSize;
                MinimumDuration = currentTrafficWatchConfiguration.MinimumDuration;
                HttpMethods = currentTrafficWatchConfiguration.HttpMethods;
                ChangeTypes = currentTrafficWatchConfiguration.ChangeTypes;
            }
        }

        public PutTrafficWatchConfigurationOperation(Parameters parameters)
        {
            _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetTrafficWatchConfigurationCommand(_parameters);
        }

        private class SetTrafficWatchConfigurationCommand : RavenCommand
        {
            private readonly Parameters _parameters;

            public SetTrafficWatchConfigurationCommand(Parameters parameters)
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
