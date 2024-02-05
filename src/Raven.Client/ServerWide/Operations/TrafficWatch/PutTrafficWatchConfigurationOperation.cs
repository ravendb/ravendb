using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Size = Sparrow.Size;

namespace Raven.Client.ServerWide.Operations.TrafficWatch
{
    public class PutTrafficWatchConfigurationOperation : IServerOperation
    {
        private readonly Parameters _parameters;

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

        public class Parameters
        {
            /// <summary>
            /// Traffic Watch logging mode.
            /// </summary>
            public TrafficWatchMode TrafficWatchMode { get; set; }

            /// <summary>
            /// Database names by which the Traffic Watch logging entities will be filtered.
            /// </summary>
            public List<string> Databases { get; set; }

            /// <summary>
            /// Response status codes by which the Traffic Watch logging entities will be filtered.
            /// </summary>
            public List<int> StatusCodes { get; set; }

            /// <summary>
            /// Minimum response size by which the Traffic Watch logging entities will be filtered.
            /// </summary>
            public Size MinimumResponseSizeInBytes { get; set; }

            /// <summary>
            /// Minimum request size by which the Traffic Watch logging entities will be filtered.
            /// </summary>
            public Size MinimumRequestSizeInBytes { get; set; }

            /// <summary>
            /// Minimum duration by which the Traffic Watch logging entities will be filtered.
            /// </summary>
            public long MinimumDurationInMs { get; set; }

            /// <summary>
            /// Request HTTP methods by which the Traffic Watch logging entities will be filtered.
            /// </summary>
            public List<string> HttpMethods { get; set; }

            /// <summary>
            /// Traffic Watch change types by which the Traffic Watch logging entities will be filtered.
            /// </summary>
            public List<TrafficWatchChangeType> ChangeTypes { get; set; }

            /// <summary>
            /// Traffic Watch certificate thumbprints by which the Traffic Watch logging entities will be filtered.
            /// </summary>
            public List<string> CertificateThumbprints { get; set; }
            
            /// <summary>
            /// Indicates if the configuration should be persisted to the configuration file
            /// </summary>
            public bool Persist { get; set; }
        }
    }
}
