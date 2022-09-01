using System.Text;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Changes;
using Raven.Client.ServerWide.Operations.TrafficWatch;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Sparrow;
using Sparrow.Logging;
using Size = Sparrow.Size;


namespace Raven.Server.TrafficWatch;

internal class TrafficWatchToLog
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenServerStartup>("TrafficWatchManager");

    public TrafficWatchConfiguration Configuration { get; private set; } = new();

    public bool LogToFile => Configuration.TrafficWatchMode == TrafficWatchMode.ToLogFile && Logger.IsOperationsEnabled;

    private TrafficWatchToLog() { }

    public static TrafficWatchToLog Instance = new TrafficWatchToLog();

    public void Log(TrafficWatchChangeBase trafficWatchData)
    {
        if (Configuration.TrafficWatchMode == TrafficWatchMode.Off)
            return;

        if (Logger.IsOperationsEnabled == false)
            return;

        var stringBuilder = new StringBuilder();
        
        if (trafficWatchData is TrafficWatchHttpChange twhc)
        {
            if (Configuration.Databases != null)
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
            var customInfo = twhc.CustomInfo?.ReplaceLineEndings(string.Empty) ?? "N/A";

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
        Configuration = configuration;
    }
}
