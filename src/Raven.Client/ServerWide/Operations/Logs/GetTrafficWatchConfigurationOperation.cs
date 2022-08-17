using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Size = Sparrow.Size;

namespace Raven.Client.ServerWide.Operations.Logs;

public class GetTrafficWatchConfigurationOperation : IServerOperation<TrafficWatchConfigurationResult>
{
    public RavenCommand<TrafficWatchConfigurationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetTrafficWatchConfigurationCommand();
    }

    public class GetTrafficWatchConfigurationCommand : RavenCommand<TrafficWatchConfigurationResult>
    {
        public override bool IsReadRequest => true;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/traffic-watch/configuration";

            return new HttpRequestMessage(HttpMethod.Get, url);
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.GetTrafficWatchConfigurationResult(response);
        }
    }
}

public class TrafficWatchConfigurationResult
{
    /// <summary>
    /// Traffic Watch logging mode.
    /// </summary>
    public TrafficWatchMode TrafficWatchMode { get; set; }

    /// <summary>
    /// Database names by which the Traffic Watch logging entities will be filtered.
    /// </summary>
    public HashSet<string> Databases { get; set; }

    /// <summary>
    /// Response status codes by which the Traffic Watch logging entities will be filtered.
    /// </summary>
    public HashSet<int> StatusCodes { get; set; }

    /// <summary>
    /// Minimum response size by which the Traffic Watch logging entities will be filtered.
    /// </summary>
    public Size MinimumResponseSize { get; set; }

    /// <summary>
    /// Minimum request size by which the Traffic Watch logging entities will be filtered.
    /// </summary>
    public Size MinimumRequestSize { get; set; }

    /// <summary>
    /// Minimum duration by which the Traffic Watch logging entities will be filtered.
    /// </summary>
    public long MinimumDuration { get; set; }

    /// <summary>
    /// Request HTTP methods by which the Traffic Watch logging entities will be filtered.
    /// </summary>
    public HashSet<string> HttpMethods { get; set; }

    /// <summary>
    /// Traffic Watch change types by which the Traffic Watch logging entities will be filtered.
    /// </summary>
    public HashSet<TrafficWatchChangeType> ChangeTypes { get; set; }
}

[Flags]
public enum TrafficWatchMode
{
    Off,
    ToLogFile
}
