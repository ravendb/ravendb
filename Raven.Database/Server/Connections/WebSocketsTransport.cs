using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Owin;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Common;
using Raven.Database.Config;
using Raven.Database.Server.Abstractions;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Connections
{
    /*
     * This is really ugly way to go about it, but that is the interface that OWIN
     * gives us
     * http://owin.org/extensions/owin-WebSocket-Extension-v0.4.0.htm
     * 
    */
    using WebSocketAccept = Action<IDictionary<string, object>, // options
        Func<IDictionary<string, object>, Task>>; // callback
    using WebSocketCloseAsync =
        Func<int /* closeStatus */,
            string /* closeDescription */,
            CancellationToken /* cancel */,
            Task>;
    using WebSocketReceiveAsync =
        Func<ArraySegment<byte> /* data */,
            CancellationToken /* cancel */,
            Task<Tuple<int /* messageType */,
                bool /* endOfMessage */,
                int /* count */>>>;
    using WebSocketSendAsync =
        Func<ArraySegment<byte> /* data */,
            int /* messageType */,
            bool /* endOfMessage */,
            CancellationToken /* cancel */,
            Task>;
    using WebSocketReceiveResult = Tuple<int, // type
        bool, // end of message?
        int>; // count

    public static class WebSocketTransportFactory 
    {
        public const string ChangesApiWebsocketSuffix = "/changes/websocket";
        public const string WatchTrafficWebsocketSuffix = "/traffic-watch/websocket";
        public const string AdminLogsWebsocketSuffix = "/admin/logs/events";
        public const string WebsocketValidateSuffix = "/websocket/validate";

        private static CancellationTokenSource ravenGcCancellation = new CancellationTokenSource();
        public static CancellationTokenSource RavenGcCancellation
        {
            get { return ravenGcCancellation; }
        }

        private class DisconnectWebSockets : ILowMemoryHandler
        {
            public static readonly DisconnectWebSockets Instance = new DisconnectWebSockets();
            

            public LowMemoryHandlerStatistics HandleLowMemory()
            {
                SoftMemoryRelease();
                return new LowMemoryHandlerStatistics();
            }

            public void SoftMemoryRelease()
            {
                var cancellationTokenSource = ravenGcCancellation;
                ravenGcCancellation = new CancellationTokenSource();
                cancellationTokenSource.Cancel();
            }

            public LowMemoryHandlerStatistics GetStats()
            {
                return new LowMemoryHandlerStatistics
                {
                    EstimatedUsedMemory = 0,
                    Name = "DisconnectWebSockets",
                    Metadata = new 
                    {
                        WebsocketTransportsCount = webSocektTransportsCount
                    }
                };
            }
        };

        static WebSocketTransportFactory()
        {
            MemoryStatistics.RegisterLowMemoryHandler(DisconnectWebSockets.Instance);
        }

        private static int webSocektTransportsCount = 0;

        public static void DecrementWebSocketTransportsCount()
        {
            Interlocked.Decrement(ref webSocektTransportsCount);
        }
        public static WebSocketsTransport CreateWebSocketTransport(RavenDBOptions options, IOwinContext context)
        {
            try
            {
                if (RavenGC.GcCollectLock.TryEnterReadLock(5000) == false)
                    throw new TimeoutException("Could not create a new web socket connection. Probably because GC is currently in progress, try again later.");
                Interlocked.Increment(ref webSocektTransportsCount);
                var localPath = context.Request.Path.Value;
                if (localPath.EndsWith(ChangesApiWebsocketSuffix))
                {
                    return new WebSocketsTransport(options, context, ravenGcCancellation.Token);
                }
                if (localPath.EndsWith(WatchTrafficWebsocketSuffix))
                {
                    return new WatchTrafficWebsocketTransport(options, context, ravenGcCancellation.Token);
                }
                if (localPath.EndsWith(AdminLogsWebsocketSuffix))
                {
                    return new AdminLogsWebsocketTransport(options, context, ravenGcCancellation.Token);
                }
                if (localPath.EndsWith(WebsocketValidateSuffix))
                {
                    return new WebSocketsValidateTransport(options, context, ravenGcCancellation.Token);
                }
                Interlocked.Decrement(ref webSocektTransportsCount);
                return null;
            }
            finally
            {
                if (RavenGC.GcCollectLock.IsReadLockHeld)
                    RavenGC.GcCollectLock.ExitReadLock();
            }
        }
    }

    public class WebSocketsValidateTransport : WebSocketsTransport
    {
        public WebSocketsValidateTransport(RavenDBOptions options, IOwinContext context, CancellationToken disconnectBecauseOfGcToken)
            : base(options, context, disconnectBecauseOfGcToken)
        {
        }

        protected override WebSocketsRequestParser CreateWebSocketsRequestParser()
        {
            return new WebSocketsRequestParser(_options.DatabaseLandlord, _options.TimeSeriesLandlord, _options.CountersLandlord, _options.FileSystemLandlord, _options.MixedModeRequestAuthorizer, WebSocketTransportFactory.WebsocketValidateSuffix);
        }

        public override async Task Run(IDictionary<string, object> websocketContext)
        {
            var sendAsync = (WebSocketSendAsync)websocketContext["websocket.SendAsync"];
            var closeAsync = (WebSocketCloseAsync)websocketContext["websocket.CloseAsync"];
            var callCancelled = (CancellationToken)websocketContext["websocket.CallCancelled"];

            int statusCode = 200;
            string statusMessage = "OK";

            try
            {
                var parser = WebSocketsRequestParser;
                await parser.ParseWebSocketRequestAsync(_context.Request.Uri, _context.Request.Query["singleUseAuthToken"]).ConfigureAwait(false);
            }
            catch (WebSocketsRequestParser.WebSocketRequestValidationException e)
            {
                statusCode = (int)e.StatusCode;
                statusMessage = string.IsNullOrEmpty(e.Message) == false ? e.Message : string.Empty;
            }

            using (var memoryStream = new MemoryStream())
            {
                var serializer = JsonExtensions.CreateDefaultJsonSerializer();

                await SendMessage(memoryStream, serializer, new { StatusCode = statusCode, StatusMessage = statusMessage, Time = SystemTime.UtcNow }, sendAsync, callCancelled).ConfigureAwait(false);
            }

            try
            {
                var buffer = new ArraySegment<byte>(new byte[1024]);
                var receiveAsync = (WebSocketReceiveAsync)websocketContext["websocket.ReceiveAsync"];
                var receiveResult = await receiveAsync(buffer, callCancelled).ConfigureAwait(false);

                if (receiveResult.Item1 == WebSocketCloseMessageType)
                {
                    var clientCloseStatus = (int)websocketContext["websocket.ClientCloseStatus"];
                    var clientCloseDescription = (string)websocketContext["websocket.ClientCloseDescription"];

                    if (clientCloseStatus == NormalClosureCode && clientCloseDescription == NormalClosureMessage)
                    {
                        await closeAsync(clientCloseStatus, clientCloseDescription, callCancelled).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception e)
            {
                Log.WarnException("Error when receiving message from web socket transport", e);
                throw;
            }
        }

        public override Task<bool> TrySetupRequest()
        {
            return new CompletedTask<bool>(true);
        }
    }

    public class WebSocketsTransport : IEventsTransport
    {
        protected static ILog Log = LogManager.GetCurrentClassLogger();

        [CLSCompliant(false)]
        protected readonly IOwinContext _context;
        private readonly CancellationToken disconnectBecauseOfGcToken;

        [CLSCompliant(false)]
        protected readonly RavenDBOptions _options;

        private readonly AsyncManualResetEvent manualResetEvent = new AsyncManualResetEvent();

        private readonly ConcurrentQueue<object> msgs = new ConcurrentQueue<object>();



        protected const int WebSocketCloseMessageType = 8;
        protected const int NormalClosureCode = 1000;
        protected const string NormalClosureMessage = "CLOSE_NORMAL";

        public string Id { get; private set; }
        public bool Connected { get; set; }
        public long CoolDownWithDataLossInMiliseconds { get; set; }

        private long lastMessageSentTick = 0;
        private object lastMessageEnqueuedAndNotSent = null;

        private WebSocketsRequestParser webSocketsRequestParser;

        protected IResourceStore ActiveTenant { get; set; }
        public string ResourceName { get; set; }

        public ArraySegment<byte> PreAllocatedBuffer;

        public WebSocketsTransport(RavenDBOptions options, IOwinContext context, CancellationToken disconnectBecauseOfGcToken)
        {
            _options = options;
            _context = context;
            this.disconnectBecauseOfGcToken = disconnectBecauseOfGcToken;
            Connected = true;
            Id = context.Request.Query["id"];
            long waitTimeBetweenMessages = 0;
            long.TryParse(context.Request.Query["coolDownWithDataLoss"], out waitTimeBetweenMessages);
            CoolDownWithDataLossInMiliseconds = waitTimeBetweenMessages;
            PreAllocatedBuffer = WebSocketBufferPool.Instance.TakeBuffer();
        }

        protected virtual WebSocketsRequestParser CreateWebSocketsRequestParser()
        {
            return new WebSocketsRequestParser(_options.DatabaseLandlord, _options.TimeSeriesLandlord, _options.CountersLandlord, _options.FileSystemLandlord, _options.MixedModeRequestAuthorizer, WebSocketTransportFactory.ChangesApiWebsocketSuffix);
        }

        public WebSocketsRequestParser WebSocketsRequestParser
        {
            get
            {
                return webSocketsRequestParser ?? (webSocketsRequestParser = CreateWebSocketsRequestParser());
            }
        }

        public void Dispose()
        {
            cancellationTokenSource.Cancel();
        }

        private readonly DateTime _started = SystemTime.UtcNow;
        private CancellationTokenSource cancellationTokenSource;
        public TimeSpan Age
        {
            get
            {
                return SystemTime.UtcNow - _started;
            }
        }

        public event Action Disconnected;

        public void SendAsync(object msg)
        {
            msgs.Enqueue(msg);
            manualResetEvent.Set();
        }

        public virtual async Task Run(IDictionary<string, object> websocketContext)
        {
            Exception captured = null;
            try
            {
                await RunInternal(websocketContext).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                captured = e;
            }
            await CloseConnection(websocketContext).ConfigureAwait(false);
            if (captured != null)
                throw new InvalidOperationException("Failure when running web sockets", captured);
        }

        private async Task RunInternal(IDictionary<string, object> websocketContext)
        {
            try
            {
                var sendAsync = (WebSocketSendAsync) websocketContext["websocket.SendAsync"];
                cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource((CancellationToken) websocketContext["websocket.CallCancelled"], disconnectBecauseOfGcToken);

                var memoryStream = new MemoryStream();
                var serializer = JsonExtensions.CreateDefaultJsonSerializer();

                CreateWaitForClientCloseTask(websocketContext);

                while (cancellationTokenSource.IsCancellationRequested == false)
                {
                    var result = await manualResetEvent.WaitAsync(5000, cancellationTokenSource.Token).ConfigureAwait(false);
                    if (cancellationTokenSource.IsCancellationRequested)
                        break;

                    if (result == false)
                    {
                        await SendMessage(memoryStream, serializer,
                            new {Type = "Heartbeat", Time = SystemTime.UtcNow},
                            sendAsync, cancellationTokenSource.Token).ConfigureAwait(false);

                        if (lastMessageEnqueuedAndNotSent != null)
                        {
                            await SendMessage(memoryStream, serializer, lastMessageEnqueuedAndNotSent, sendAsync, cancellationTokenSource.Token).ConfigureAwait(false);
                            lastMessageEnqueuedAndNotSent = null;
                            lastMessageSentTick = Environment.TickCount;
                        }
                        continue;
                    }

                    manualResetEvent.Reset();

                    object message;
                    while (msgs.TryDequeue(out message))
                    {
                        if (cancellationTokenSource.IsCancellationRequested)
                            break;

                        if (CoolDownWithDataLossInMiliseconds > 0 && Environment.TickCount - lastMessageSentTick < CoolDownWithDataLossInMiliseconds)
                        {
                            lastMessageEnqueuedAndNotSent = message;
                            continue;
                        }

                        await SendMessage(memoryStream, serializer, message, sendAsync, cancellationTokenSource.Token).ConfigureAwait(false);
                        lastMessageEnqueuedAndNotSent = null;
                        lastMessageSentTick = Environment.TickCount;
                    }
                }
            }
            catch (Exception e)
            {
                Log.Info("Error when handling web socket connection", e);
                if (cancellationTokenSource != null)
                    cancellationTokenSource.Cancel();
            }
            finally
            {
                OnDisconnection();
            }
        }

        private async Task CloseConnection(IDictionary<string, object> websocketContext)
        {
            if (EnvironmentUtils.RunningOnPosix)
            {
                var closeAsync = (WebSocketCloseAsync) websocketContext["websocket.CloseAsync"];
                WebSocketCloseStatus status = WebSocketCloseStatus.EndpointUnavailable;
                var token =
                    CancellationTokenSource.CreateLinkedTokenSource((CancellationToken) websocketContext["websocket.CallCancelled"],
                        disconnectBecauseOfGcToken).Token;
                try
                {
                    await closeAsync((int) status, "Heartbeat bad response", token).ConfigureAwait(false);
                }
                catch
                {
                    // nothing to do if we can't catch the error
                }
            }
        }

        private void CreateWaitForClientCloseTask(IDictionary<string, object> websocketContext)
        {
            new Task(async () =>
            {
                var buffer = new ArraySegment<byte>(new byte[1024]);
                var receiveAsync = (WebSocketReceiveAsync)websocketContext["websocket.ReceiveAsync"];
                var closeAsync = (WebSocketCloseAsync)websocketContext["websocket.CloseAsync"];

                while (true)
                {
                    try
                    {
                        bool cancelled = false;
                        WebSocketReceiveResult receiveResult = null;
                        try
                        {
                            receiveResult = await receiveAsync(buffer, cancellationTokenSource.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            cancelled = true;
                        }
                        if (cancelled)
                        {
                            try
                            {
                                await closeAsync((int)WebSocketCloseStatus.NormalClosure, "Closed for GC release", CancellationToken.None).ConfigureAwait(false);
                                manualResetEvent.Set();
                            }
                            catch (Exception e)
                            {
                                Log.WarnException("Error when closing connection web socket transport", e);
                            }
                            return;
                        }

                        if (receiveResult.Item1 == WebSocketCloseMessageType)
                        {
                            var clientCloseStatus = (int)websocketContext["websocket.ClientCloseStatus"];
                            var clientCloseDescription = (string)websocketContext["websocket.ClientCloseDescription"];

                            if (clientCloseStatus == NormalClosureCode && clientCloseDescription == NormalClosureMessage)
                            {
                                await closeAsync(clientCloseStatus, clientCloseDescription, cancellationTokenSource.Token).ConfigureAwait(false);
                                manualResetEvent.Set();
                            }

                            //At this point the WebSocket is in a 'CloseReceived' state, so there is no need to continue waiting for messages
                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        Log.WarnException("Error when receiving message from web socket transport", e);
                        return;
                    }
                }
            }).Start();
        }

        protected virtual async Task SendMessage(MemoryStream memoryStream, JsonSerializer serializer, object message, WebSocketSendAsync sendAsync, CancellationToken callCancelled)
        {
            memoryStream.Position = 0;
            var jsonTextWriter = new JsonTextWriter(new StreamWriter(memoryStream));
            serializer.Serialize(jsonTextWriter, message);
            jsonTextWriter.Flush();

            var arraySegment = new ArraySegment<byte>(memoryStream.GetBuffer(), 0, (int)memoryStream.Position);
            await sendAsync(arraySegment, 1, true, callCancelled).ConfigureAwait(false);
        }

        private void OnDisconnection()
        {
            Connected = false;
            try
            {
                Action onDisconnected = Disconnected;
                if (onDisconnected != null)
                    onDisconnected();
            }
            finally
            {
                WebSocketBufferPool.Instance.ReturnBuffer(PreAllocatedBuffer);
            }
        }

        public virtual async Task<bool> TrySetupRequest()
        {
            try
            {
                var parser = WebSocketsRequestParser;
                var request = await parser.ParseWebSocketRequestAsync(_context.Request.Uri, _context.Request.Query["singleUseAuthToken"]).ConfigureAwait(false);

                Id = request.Id;
                ActiveTenant = request.ActiveResource;
                ResourceName = request.ResourceName;
            }
            catch (WebSocketsRequestParser.WebSocketRequestValidationException e)
            {
                _context.Response.StatusCode = (int)e.StatusCode;
                _context.Response.ReasonPhrase = e.StatusCode.ToString();
                _context.Response.Write(string.Format("{{ 'Error': '{0}' }}", e.Message));

                return false;
            }

            RegisterTransportState();
            return true;
        }

        protected virtual void RegisterTransportState()
        {
            ActiveTenant.TransportState.Register(this);
        }
    }

    public class WatchTrafficWebsocketTransport : WebSocketsTransport
    {
        public WatchTrafficWebsocketTransport(RavenDBOptions options, IOwinContext context, CancellationToken disconnectBecauseOfGcToken)
            : base(options, context, disconnectBecauseOfGcToken)
        {
        }

        protected override WebSocketsRequestParser CreateWebSocketsRequestParser()
        {
            return new WatchTrafficWebSocketsRequestParser(_options.DatabaseLandlord, _options.TimeSeriesLandlord, _options.CountersLandlord, _options.FileSystemLandlord, _options.MixedModeRequestAuthorizer, WebSocketTransportFactory.WatchTrafficWebsocketSuffix);
        }

        protected override void RegisterTransportState()
        {
            if (ResourceName != Constants.SystemDatabase)
            {
                _options.RequestManager.RegisterResourceHttpTraceTransport(this, ResourceName);
            }
            else
            {
                _options.RequestManager.RegisterServerHttpTraceTransport(this);
            }
        }
    }

    public class AdminLogsWebsocketTransport : WebSocketsTransport
    {
        public AdminLogsWebsocketTransport(RavenDBOptions options, IOwinContext context, CancellationToken disconnectBecauseOfGcToken)
            : base(options, context, disconnectBecauseOfGcToken)
        {
        }

        protected override WebSocketsRequestParser CreateWebSocketsRequestParser()
        {
            return new AdminLogsWebSocketsRequestParser(_options.DatabaseLandlord, _options.TimeSeriesLandlord, _options.CountersLandlord, _options.FileSystemLandlord, _options.MixedModeRequestAuthorizer, WebSocketTransportFactory.AdminLogsWebsocketSuffix);
        }

        protected override Task SendMessage(MemoryStream memoryStream, JsonSerializer serializer, object message, WebSocketSendAsync sendAsync, CancellationToken callCancelled)
        {
            var typedMessage = message as LogEventInfo;

            if (typedMessage != null)
            {
                message = new LogEventInfoFormatted(typedMessage);
            }

            return base.SendMessage(memoryStream, serializer, message, sendAsync, callCancelled);
        }

        protected override void RegisterTransportState()
        {
            var logTarget = LogManager.GetTarget<AdminLogsTarget>();
            logTarget.Register(this);
        }
    }
}
