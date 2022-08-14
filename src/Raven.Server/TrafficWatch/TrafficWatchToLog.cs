using System;
using System.Collections.Generic;
using Raven.Client.Documents.Changes;
using Raven.Server.Config.Categories;
using Sparrow;
using Sparrow.Logging;


namespace Raven.Server.TrafficWatch;

internal class TrafficWatchToLog
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenServerStartup>("TrafficWatchManager");

    private TrafficWatchConfiguration _configuration = new();

    public bool LogToFile => _configuration.TrafficWatchMode == TrafficWatchMode.ToLogFile || Logger.IsOperationsEnabled;

    private TrafficWatchToLog() { }

    public static TrafficWatchToLog Instance = new TrafficWatchToLog();

    public void Log(TrafficWatchChangeBase trafficWatchData)
    {
        UpdateConfiguration(new TrafficWatchConfiguration
        {
            TrafficWatchMode = TrafficWatchMode.ToLogFile,
            Databases = new HashSet<string> { "test" },
            StatusCodes = new HashSet<int?> { 202 },
            MinimumResponseSize = new Size(10, SizeUnit.Bytes),
            MinimumRequestSize = new Size(2, SizeUnit.Bytes),
            MinimumDuration = 5,
            HttpMethods = new HashSet<string> { "POST" },
            ChangeTypes = new HashSet<TrafficWatchChangeType> { TrafficWatchChangeType.Queries }
        });

        if (Logger.IsOperationsEnabled == false)
            return;

        if (_configuration.TrafficWatchMode == TrafficWatchMode.Off)
            return;

        try
        {
            if (trafficWatchData is TrafficWatchHttpChange twhc)
            {
                if (_configuration.Databases != null &&
                    _configuration.Databases.Contains(twhc.DatabaseName) == false)
                    return;

                if (_configuration.MinimumResponseSize.GetValue(SizeUnit.Bytes) > twhc.ResponseSizeInBytes)
                    return;

                if (_configuration.MinimumRequestSize.GetValue(SizeUnit.Bytes) > twhc.RequestSizeInBytes)
                    return;

                if (_configuration.HttpMethods != null &&
                    _configuration.HttpMethods.Contains(twhc.HttpMethod) == false)
                    return;

                if (_configuration.MinimumDuration > twhc.ElapsedMilliseconds)
                    return;

                if (_configuration.ChangeTypes != null &&
                    _configuration.ChangeTypes.Contains(twhc.Type) == false)
                    return;

                if (_configuration.StatusCodes != null &&
                    _configuration.StatusCodes.Contains(twhc.ResponseStatusCode) == false)
                    return;

                var requestSize = new Size(twhc.RequestSizeInBytes, SizeUnit.Bytes);
                var responseSize = new Size(twhc.ResponseSizeInBytes, SizeUnit.Bytes);
                var customInfo = twhc.CustomInfo?.ReplaceLineEndings("") ?? "N/A";

                string msg = $"HTTP, {twhc.TimeStamp:MM/dd/yyyy HH:mm:ss}, {twhc.DatabaseName}, " +
                             $"{twhc.ClientIP}, {twhc.CertificateThumbprint ?? "N/A"}, " +
                             $"request ID: {twhc.RequestId}, {twhc.HttpMethod}, {twhc.ResponseStatusCode}, " +
                             $"{twhc.RequestUri}, {twhc.AbsoluteUri}, request: {requestSize}, " +
                             $"response: {responseSize}, {twhc.Type}, {twhc.ElapsedMilliseconds}ms, " +
                             $"custom info: [{customInfo}]";

                if (_configuration.TrafficWatchMode == TrafficWatchMode.ToLogFile)
                    Logger.Operations(msg);
            }
            else if (trafficWatchData is TrafficWatchTcpChange twtc)
            {
                string msg = $"TCP, {twtc.TimeStamp:MM/dd/yyyy HH:mm:ss}, {twtc.Operation}, " +
                             $"{twtc.OperationVersion}, {twtc.DatabaseName}, {twtc.Source}, " +
                             $"{twtc.CustomInfo}, {twtc.ClientIP}, {twtc.CertificateThumbprint}";

                if (_configuration.TrafficWatchMode == TrafficWatchMode.ToLogFile)
                    Logger.Info(msg);
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }


    public void UpdateConfiguration(TrafficWatchConfiguration configuration)
    {
        _configuration = configuration;
    }
}
