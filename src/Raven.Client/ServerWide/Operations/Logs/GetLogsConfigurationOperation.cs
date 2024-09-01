using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Client.ServerWide.Operations.Logs
{
    public sealed class GetLogsConfigurationOperation : IServerOperation<GetLogsConfigurationResult>
    {
        public RavenCommand<GetLogsConfigurationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetLogsConfigurationCommand();
        }

        private class GetLogsConfigurationCommand : RavenCommand<GetLogsConfigurationResult>
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
        public LogsConfiguration Logs { get; set; }

        public AuditLogsConfiguration AuditLogs { get; set; }

        public MicrosoftLogsConfiguration MicrosoftLogs { get; set; }

        public AdminLogsConfiguration AdminLogs { get; set; }
    }

    public sealed class LogsConfiguration : IDynamicJson
    {
        public string Path { get; set; }

        public LogLevel CurrentMinLevel { get; set; }

        public LogLevel CurrentMaxLevel { get; set; }

        public LogLevel MinLevel { get; set; }

        public LogLevel MaxLevel { get; set; }

        public long ArchiveAboveSizeInMb { get; set; }

        public int? MaxArchiveDays { get; set; }

        public int? MaxArchiveFiles { get; set; }

        public bool EnableArchiveFileCompression { get; set; }

        public List<LogFilter> Filters { get; set; } = new();

        public LogFilterAction LogFilterDefaultAction { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Path)] = Path,
                [nameof(CurrentMinLevel)] = CurrentMinLevel,
                [nameof(CurrentMaxLevel)] = CurrentMaxLevel,
                [nameof(MinLevel)] = MinLevel,
                [nameof(MaxLevel)] = MaxLevel,
                [nameof(ArchiveAboveSizeInMb)] = ArchiveAboveSizeInMb,
                [nameof(MaxArchiveDays)] = MaxArchiveDays,
                [nameof(MaxArchiveFiles)] = MaxArchiveFiles,
                [nameof(EnableArchiveFileCompression)] = EnableArchiveFileCompression,
                [nameof(Filters)] = new DynamicJsonArray(Filters.Select(x => x.ToJson())),
                [nameof(LogFilterDefaultAction)] = LogFilterDefaultAction
            };
        }
    }

    public sealed class AuditLogsConfiguration : IDynamicJson
    {
        public string Path { get; set; }

        public LogLevel Level { get; set; }

        public long ArchiveAboveSizeInMb { get; set; }

        public int? MaxArchiveDays { get; set; }

        public int? MaxArchiveFiles { get; set; }

        public bool EnableArchiveFileCompression { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Path)] = Path,
                [nameof(Level)] = Level,
                [nameof(ArchiveAboveSizeInMb)] = ArchiveAboveSizeInMb,
                [nameof(MaxArchiveDays)] = MaxArchiveDays,
                [nameof(MaxArchiveFiles)] = MaxArchiveFiles,
                [nameof(EnableArchiveFileCompression)] = EnableArchiveFileCompression,
            };
        }
    }

    public sealed class MicrosoftLogsConfiguration : IDynamicJson
    {
        public LogLevel CurrentMinLevel { get; set; }

        public LogLevel MinLevel { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(CurrentMinLevel)] = CurrentMinLevel,
                [nameof(MinLevel)] = MinLevel
            };
        }
    }

    public sealed class AdminLogsConfiguration : IDynamicJson
    {
        public LogLevel CurrentMinLevel { get; set; }

        public LogLevel CurrentMaxLevel { get; set; }

        public List<LogFilter> Filters { get; set; } = new();

        public LogFilterAction LogFilterDefaultAction { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(CurrentMinLevel)] = CurrentMinLevel,
                [nameof(CurrentMaxLevel)] = CurrentMaxLevel,
                [nameof(Filters)] = new DynamicJsonArray(Filters.Select(x => x.ToJson())),
                [nameof(LogFilterDefaultAction)] = LogFilterDefaultAction
            };
        }
    }
}
