using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.ServerWide.Operations.Logs;

public class GetTrafficWatchConfiguration : IServerOperation<GetTrafficWatchConfigurationResult>
{
    public RavenCommand<GetTrafficWatchConfigurationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetTrafficWatchConfigurationCommand();
    }

    public class GetTrafficWatchConfigurationCommand : RavenCommand<GetTrafficWatchConfigurationResult>
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

            Result = JsonDeserializationClient.GetTrafficWatchesConfigurationResult(response);
        }
    }
}

public class GetTrafficWatchConfigurationResult
{
    /// <summary>
    /// Current mode that is active
    /// </summary>
    public LogMode CurrentMode { get; set; }

    /// <summary>
    /// Mode that is written in the configuration file and which will be used after server restart
    /// </summary>
    public TrafficWatchMode TrafficWatchMode { get; set; }

    /// <summary>
    /// Path to which logs will be written
    /// </summary>
    public string Path { get; set; }

    /// <summary>
    /// Filter by Database names for which the operation should be logged
    /// </summary>
    public HashSet<string> Databases { get; set; }
    
    /// <summary>
    /// Filter by HTTP response status codes for which the operation should be logged
    /// </summary>
    public HashSet<int?> StatusCodes { get; set; }

    /// <summary>
    /// Filter by minimum response size for which the operation should be logged
    /// </summary>
    public Size MinimumResponseSize { get; set; }

    /// <summary>
    /// Filter by minimum request size for which the operation should be logged
    /// </summary>
    public Size MinimumRequestSize { get; set; }

  /// <summary>
    /// Filter by minimum duration for which the operation should be logged
    /// </summary>
    public long MinimumDuration { get; set; }

    /// <summary>
    /// Filter by HTTP request method for which the operation should be logged
    /// </summary>
    public HashSet<string> HttpMethods { get; set; }

    /// <summary>
    /// Filter by type of traffic watch change for which the operation should be logged
    /// </summary>
    public HashSet<TrafficWatchChangeType> ChangeTypes { get; set; }

    /// <summary>
    /// The maximum size of the log after which the old files will be deleted
    /// </summary>
    public Size MaxFileSize { get; set; }

    /// <summary>
    /// Logs retention time
    /// </summary>
    public TimeSpan RetentionTime { get; set; }

    /// <summary>
    /// Logs retention size (null if RetentionSize is long.MaxValue)
    /// </summary>
    public Size RetentionSize { get; set; }
    
    /// <summary>
    /// Are logs compressed
    /// </summary>
    public bool Compress { get; set; }
}
