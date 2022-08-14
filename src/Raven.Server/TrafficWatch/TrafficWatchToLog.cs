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

    public TrafficWatchConfiguration Configuration { get; private set; } = new();

    public bool LogToFile => Configuration.TrafficWatchMode == TrafficWatchMode.ToLogFile || Logger.IsOperationsEnabled;

    private TrafficWatchToLog() { }

    public static TrafficWatchToLog Instance = new TrafficWatchToLog();

    public void Log(TrafficWatchChangeBase trafficWatchData)
    {
        
        if (Logger.IsOperationsEnabled == false)
            return;

        if (Configuration.TrafficWatchMode == TrafficWatchMode.Off)
            return;

        try
        {
            if (trafficWatchData is TrafficWatchHttpChange twhc)
            {
                if(Configuration.Databases != null)
                    if (Configuration.Databases.Count > 0 && 
                        Configuration.Databases.Contains(twhc.DatabaseName) == false)
                        return;

                if (Configuration.HttpMethods != null)
                    if (Configuration.HttpMethods.Count > 0 &&
                        Configuration.HttpMethods.Contains(twhc.HttpMethod) == false)
                        return;

                if (Configuration.ChangeTypes != null)
                    if (Configuration.ChangeTypes.Count > 0 &&
                        Configuration.ChangeTypes.Contains(twhc.Type) == false)
                        return;

                if (Configuration.StatusCodes != null)
                    if (Configuration.StatusCodes.Count > 0 &&
                        Configuration.StatusCodes.Contains(twhc.ResponseStatusCode) == false)
                        return;

                if (Configuration.MinimumResponseSize.GetValue(SizeUnit.Bytes) > twhc.ResponseSizeInBytes)
                    return;

                if (Configuration.MinimumRequestSize.GetValue(SizeUnit.Bytes) > twhc.RequestSizeInBytes)
                    return;

                if (Configuration.MinimumDuration > twhc.ElapsedMilliseconds)
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

                if (Configuration.TrafficWatchMode == TrafficWatchMode.ToLogFile)
                    Logger.Operations(msg);
            }
            else if (trafficWatchData is TrafficWatchTcpChange twtc)
            {
                string msg = $"TCP, {twtc.TimeStamp:MM/dd/yyyy HH:mm:ss}, {twtc.Operation}, " +
                             $"{twtc.OperationVersion}, {twtc.DatabaseName}, {twtc.Source}, " +
                             $"{twtc.CustomInfo}, {twtc.ClientIP}, {twtc.CertificateThumbprint}";

                if (Configuration.TrafficWatchMode == TrafficWatchMode.ToLogFile)
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
        Configuration = configuration;
    }
}
