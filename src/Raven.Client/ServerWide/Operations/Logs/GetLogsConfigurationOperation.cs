﻿using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.ServerWide.Operations.Logs
{
    public sealed class GetLogsConfigurationOperation : IServerOperation<GetLogsConfigurationResult>
    {
        public RavenCommand<GetLogsConfigurationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetLogsConfigurationCommand();
        }

        private sealed class GetLogsConfigurationCommand : RavenCommand<GetLogsConfigurationResult>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/logs/configuration";

                return new HttpRequestMessage(HttpMethod.Get, url);
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetLogsConfigurationResult(response);
            }
        }
    }

    public sealed class GetLogsConfigurationResult
    {
        /// <summary>
        /// Current mode that is active
        /// </summary>
        public LogMode CurrentMode { get; set; }

        /// <summary>
        /// Mode that is written in the configuration file and which will be used after server restart
        /// </summary>
        public LogMode Mode { get; set; }

        /// <summary>
        /// Path to which logs will be written
        /// </summary>
        public string Path { get; set; }

        /// <summary>
        /// Indicates if logs will be written in UTC or in server local time
        /// </summary>
        public bool UseUtcTime { get; set; }

        /// <summary>
        /// Logs retention time
        /// </summary>
        public TimeSpan RetentionTime { get; set; }
        
        /// <summary>
        /// Logs retention size (null if RetentionSize is long.MaxValue)
        /// </summary>
        public Size? RetentionSize { get; set; }
        
        /// <summary>
        /// Are logs compressed
        /// </summary>
        public bool Compress { get; set; }
    }
}
