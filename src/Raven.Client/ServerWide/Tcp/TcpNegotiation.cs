using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Client.ServerWide.Tcp
{
    public static class TcpNegotiation
    {
        public const int OutOfRangeStatus = -1;
        public const int DropStatus = -2;

        private static readonly Logger Log = LoggingSource.Instance.GetLogger("TCP Negotiation", typeof(TcpNegotiation).FullName);

        private static SyncTcpNegotiation _sync;

        internal static SyncTcpNegotiation Sync
        {
            get => _sync ??= new SyncTcpNegotiation();
        }

        public static async ValueTask<TcpConnectionHeaderMessage.SupportedFeatures> NegotiateProtocolVersionAsync(JsonOperationContext context, Stream stream, AsyncTcpNegotiateParameters parameters)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info($"Start negotiation for {parameters.Operation} operation with {parameters.DestinationNodeTag ?? parameters.DestinationUrl}");
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
            {
                var current = parameters.Version;
                while (true)
                {
                    if (parameters.CancellationToken.IsCancellationRequested)
                        throw new OperationCanceledException($"Stopped TCP negotiation for {parameters.Operation} because of cancellation request");

                    await SendTcpVersionInfoAsync(context, writer, parameters, current).ConfigureAwait(false);
                    var version = await parameters.ReadResponseAndGetVersionCallbackAsync(context, writer, stream, parameters.DestinationUrl).ConfigureAwait(false);
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info($"Read response from {parameters.SourceNodeTag ?? parameters.DestinationUrl} for '{parameters.Operation}', received version is '{version}'");
                    }

                    if (version == current)
                        break;

                    //In this case we usually throw internally but for completeness we better handle it
                    if (version == DropStatus)
                    {
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Drop, TcpConnectionHeaderMessage.DropBaseLine);
                    }
                    var status = TcpConnectionHeaderMessage.OperationVersionSupported(parameters.Operation, version, out current);
                    if (status == TcpConnectionHeaderMessage.SupportedStatus.OutOfRange)
                    {
                        await SendTcpVersionInfoAsync(context, writer, parameters, OutOfRangeStatus).ConfigureAwait(false);
                        throw new ArgumentException($"The {parameters.Operation} version {parameters.Version} is out of range, our lowest version is {current}");
                    }
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info($"The version {version} is {status}, will try to agree on '{current}' for {parameters.Operation} with {parameters.DestinationNodeTag ?? parameters.DestinationUrl}.");
                    }
                }
                if (Log.IsInfoEnabled)
                {
                    Log.Info($"{parameters.DestinationNodeTag ?? parameters.DestinationUrl} agreed on version '{current}' for {parameters.Operation}.");
                }
                return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(parameters.Operation, current);
            }
        }

        private static async ValueTask SendTcpVersionInfoAsync(JsonOperationContext context, AsyncBlittableJsonTextWriter writer, AsyncTcpNegotiateParameters parameters, int currentVersion)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info($"Send negotiation for {parameters.Operation} in version {currentVersion}");
            }

            context.Write(writer, new DynamicJsonValue
            {
                [nameof(TcpConnectionHeaderMessage.DatabaseName)] = parameters.Database,
                [nameof(TcpConnectionHeaderMessage.Operation)] = parameters.Operation.ToString(),
                [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = parameters.SourceNodeTag,
                [nameof(TcpConnectionHeaderMessage.OperationVersion)] = currentVersion,
                [nameof(TcpConnectionHeaderMessage.AuthorizeInfo)] = parameters.AuthorizeInfo?.ToJson()
            });

            await writer.FlushAsync().ConfigureAwait(false);
        }

        internal class SyncTcpNegotiation
        {
        }
    }

    public sealed class AsyncTcpNegotiateParameters : AbstractTcpNegotiateParameters
    {
        /// <summary>
        /// ReadResponseAndGetVersion Function should take care reading the TcpConnectionHeaderResponse respond from the input 'stream'
        /// And return the version of the supported TCP protocol.
        ///
        /// If the respond is 'Drop' the function should throw.
        /// If the respond is 'None' the function should throw.
        /// If the respond is 'TcpMismatch' the function should return the read version.
        /// </summary>
        public Func<JsonOperationContext, AsyncBlittableJsonTextWriter, Stream, string, ValueTask<int>> ReadResponseAndGetVersionCallbackAsync { get; set; }
    }

    public abstract class AbstractTcpNegotiateParameters
    {
        public TcpConnectionHeaderMessage.OperationTypes Operation { get; set; }
        public TcpConnectionHeaderMessage.AuthorizationInfo AuthorizeInfo { get; set; }
        public int Version { get; set; }
        public string Database { get; set; }
        public string SourceNodeTag { get; set; }
        public string DestinationNodeTag { get; set; }
        public string DestinationUrl { get; set; }
        public CancellationToken CancellationToken { get; set; }
    }
}
