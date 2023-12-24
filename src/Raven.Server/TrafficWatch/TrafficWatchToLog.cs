using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Changes;
using Raven.Client.ServerWide.Operations.TrafficWatch;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Size = Sparrow.Size;

namespace Raven.Server.TrafficWatch;

internal class TrafficWatchToLog : IDynamicJson
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenServerStartup>("TrafficWatchManager");

    private TrafficWatchMode _trafficWatchMode;
    private List<string> _databases;
    private List<int> _statusCodes;
    private long _minimumResponseSizeInBytes;
    private long _minimumRequestSizeInBytes;
    private long _minimumDurationInMs;
    private List<string> _httpMethods;
    private List<TrafficWatchChangeType> _changeTypes;
    private List<string> _certificateThumbprints;

    public bool LogToFile => _trafficWatchMode == TrafficWatchMode.ToLogFile && Logger.IsOperationsEnabled;

    private TrafficWatchToLog() { }

    public static TrafficWatchToLog Instance = new();

    public void Log(TrafficWatchChangeBase trafficWatchData)
    {
        if (_trafficWatchMode == TrafficWatchMode.Off)
            return;

        if (Logger.IsOperationsEnabled == false)
            return;

        var stringBuilder = new StringBuilder();
        
        if (trafficWatchData is TrafficWatchHttpChange twhc)
        {
            if (_databases is { Count: > 0 } &&
                _databases.Contains(twhc.DatabaseName) == false)
                return;

            if (_httpMethods is { Count: > 0 } &&
                _httpMethods.Contains(twhc.HttpMethod) == false)
                return;

            if (_changeTypes is { Count: > 0 } &&
                _changeTypes.Contains(twhc.Type) == false)
                return;

            if (_statusCodes is { Count: > 0 } &&
                _statusCodes.Contains(twhc.ResponseStatusCode) == false)
                return;

            if(_certificateThumbprints is { Count: > 0 } &&
               _certificateThumbprints.Contains(twhc.CertificateThumbprint) == false)
                return;

            if (_minimumResponseSizeInBytes > twhc.ResponseSizeInBytes)
                return;

            if (_minimumResponseSizeInBytes > twhc.RequestSizeInBytes)
                return;

            if (_minimumDurationInMs > twhc.ElapsedMilliseconds)
                return;

            var requestSize = new Size(twhc.RequestSizeInBytes, SizeUnit.Bytes);
            var responseSize = new Size(twhc.ResponseSizeInBytes, SizeUnit.Bytes);
            var customInfo = twhc.CustomInfo?.ReplaceLineEndings(" ") ?? "N/A";
            var queryTimings = twhc.QueryTimings;

            stringBuilder
                .Append("HTTP, ")
                .Append(twhc.DatabaseName).Append(", ")
                .Append(twhc.ClientIP).Append(", ")
                .Append(twhc.CertificateThumbprint ?? "N/A").Append(", ")
                .Append("request ID: ").Append(twhc.RequestId).Append(", ")
                .Append(twhc.HttpMethod).Append(", ")
                .Append(twhc.ResponseStatusCode).Append(", ")
                .Append(twhc.RequestUri).Append(", ")
                .Append(twhc.AbsoluteUri).Append(", ")
                .Append("request size: ").Append(requestSize).Append(", ")
                .Append("response size: ").Append(responseSize).Append(", ")
                .Append(twhc.Type).Append(", ")
                .Append(twhc.ElapsedMilliseconds).Append("ms, ")
                .Append("custom info: ").Append(customInfo);

            if (queryTimings != null)
            {
                bool isFirst = true;
                stringBuilder.Append(", ")
                    .Append("query timings: ").Append(queryTimings.DurationInMs).Append("ms - ");
                foreach (var key in queryTimings.Timings.Keys)
                {
                    if (isFirst == false)
                        stringBuilder.Append(", ");
                    stringBuilder.Append(key).Append(": ")
                        .Append(queryTimings.Timings[key].DurationInMs).Append("ms");

                    isFirst = false;
                }
            }
        }
        else if (trafficWatchData is TrafficWatchTcpChange twtc)
        {
            stringBuilder
                .Append("TCP, ")
                .Append(twtc.Operation).Append(", ")
                .Append(twtc.OperationVersion).Append(", ")
                .Append(twtc.DatabaseName).Append(", ")
                .Append(twtc.Source).Append(", ")
                .Append(twtc.CustomInfo).Append(", ")
                .Append(twtc.ClientIP).Append(", ")
                .Append(twtc.CertificateThumbprint);
        }

        Logger.Operations(stringBuilder.ToString());
    }

    public void UpdateConfiguration(TrafficWatchConfiguration configuration)
    {
        _trafficWatchMode = configuration.TrafficWatchMode;
        _databases = configuration.Databases?.ToList();
        _statusCodes = configuration.StatusCodes?.ToList();
        _minimumResponseSizeInBytes = configuration.MinimumResponseSize.GetValue(SizeUnit.Bytes);
        _minimumRequestSizeInBytes = configuration.MinimumRequestSize.GetValue(SizeUnit.Bytes);
        _minimumDurationInMs = configuration.MinimumDuration.GetValue(TimeUnit.Milliseconds);
        _httpMethods = configuration.HttpMethods?.ToList();
        _changeTypes = configuration.ChangeTypes?.ToList();
        _certificateThumbprints = configuration.CertificateThumbprints?.ToList();
    }

    public void UpdateConfiguration(PutTrafficWatchConfigurationOperation.Parameters configuration)
    {
        _trafficWatchMode = configuration.TrafficWatchMode;
        _databases = configuration.Databases;
        _statusCodes = configuration.StatusCodes;
        _minimumResponseSizeInBytes = configuration.MinimumResponseSizeInBytes.GetValue(SizeUnit.Bytes);
        _minimumRequestSizeInBytes = configuration.MinimumRequestSizeInBytes.GetValue(SizeUnit.Bytes);
        _minimumDurationInMs = configuration.MinimumDurationInMs;
        _httpMethods = configuration.HttpMethods;
        _changeTypes = configuration.ChangeTypes;
        _certificateThumbprints = configuration.CertificateThumbprints;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(PutTrafficWatchConfigurationOperation.Parameters.TrafficWatchMode)] = _trafficWatchMode,
            [nameof(PutTrafficWatchConfigurationOperation.Parameters.Databases)] = Instance._databases == null ? null : new DynamicJsonArray(Instance._databases),
            [nameof(PutTrafficWatchConfigurationOperation.Parameters.StatusCodes)] = Instance._statusCodes == null ? null : new DynamicJsonArray(Instance._statusCodes.Cast<object>()),
            [nameof(PutTrafficWatchConfigurationOperation.Parameters.MinimumResponseSizeInBytes)] = _minimumResponseSizeInBytes,
            [nameof(PutTrafficWatchConfigurationOperation.Parameters.MinimumRequestSizeInBytes)] = Instance._minimumRequestSizeInBytes,
            [nameof(PutTrafficWatchConfigurationOperation.Parameters.MinimumDurationInMs)] = Instance._minimumDurationInMs,
            [nameof(PutTrafficWatchConfigurationOperation.Parameters.HttpMethods)] = Instance._httpMethods == null ? null : new DynamicJsonArray(Instance._httpMethods),
            [nameof(PutTrafficWatchConfigurationOperation.Parameters.ChangeTypes)] = Instance._changeTypes == null ? null : new DynamicJsonArray(Instance._changeTypes.Cast<object>()),
            [nameof(PutTrafficWatchConfigurationOperation.Parameters.CertificateThumbprints)] = Instance._certificateThumbprints == null ? null : new DynamicJsonArray(Instance._certificateThumbprints)
        };
    }
}
