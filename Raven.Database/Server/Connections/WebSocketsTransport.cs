using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Owin;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Counters;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

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
        int>;
    using Raven.Database.Server.RavenFS; // count

    public class WebSocketTransportFactory
    {
        public const string CHANGES_API_WEBSOCKET_SUFFIX = "/changes/websocket";
        public const string WATCH_TRAFFIC_WEBSOCKET_SUFFIX = "/http-trace/websocket";
        public const string CUSTOM_LOGGING_WEBSOCKET_SUFFIX = "/admin/logs/events";

        public static WebSocketsTransport CreateWebSocketTransport(RavenDBOptions options, IOwinContext context)
        {
            if (context.Request.Uri.LocalPath.EndsWith(CHANGES_API_WEBSOCKET_SUFFIX))
            {
                return new WebSocketsTransport(options, context);
            }
            if (context.Request.Uri.LocalPath.EndsWith(WATCH_TRAFFIC_WEBSOCKET_SUFFIX))
            {
                return new WatchTrafficWebsocketTransport(options, context);
            }
            if (context.Request.Uri.LocalPath.EndsWith(CUSTOM_LOGGING_WEBSOCKET_SUFFIX))
            {
                return  new CustomLogWebsocketTransport(options, context);
            }
            return null;
        }
    }

    public class WebSocketsTransport : IEventsTransport
    {
	    private static ILog log = LogManager.GetCurrentClassLogger();
        
        protected readonly IOwinContext _context;
        protected readonly RavenDBOptions _options;

        private readonly AsyncManualResetEvent manualResetEvent = new AsyncManualResetEvent();

        private readonly ConcurrentQueue<object> msgs = new ConcurrentQueue<object>();

		private const int WebSocketCloseMessageType = 8;
		private const int NormalClosureCode = 1000;
		private const string NormalClosureMessage = "CLOSE_NORMAL";
        
        public string Id { get; private set; }
        public bool Connected { get; set; }
        public long CoolDownWithDataLossInMiliseconds { get; set; }

        private long lastMessageSentTick = 0;
        private object lastMessageEnqueuedAndNotSent = null;
        
        private const string COUNTERS_URL_PREFIX = "counters";
        private const string DATABASES_URL_PREFIX = "databases";
        private const string FILESYSTEMS_URL_PREFIX = "fs";

        protected IResource ActiveTenant { get; set; }
        public string ResourceName { get; set; }
        
        public WebSocketsTransport(RavenDBOptions options, IOwinContext context)
        {
            _options = options;
            _context = context;
            Connected = true;
            Id = context.Request.Query["id"];
            long waitTimeBetweenMessages = 0;
            long.TryParse(context.Request.Query["coolDownWithDataLoss"], out waitTimeBetweenMessages);
            CoolDownWithDataLossInMiliseconds = waitTimeBetweenMessages;
            
        }

        protected virtual string ExpectedRequestSuffix
        {
            get { return WebSocketTransportFactory.CHANGES_API_WEBSOCKET_SUFFIX; }
        }
        
        public void Dispose()
        {
        }

        public event Action Disconnected;

        public void SendAsync(object msg)
        {
            msgs.Enqueue(msg);
            manualResetEvent.Set();
        }

        public async Task Run(IDictionary<string, object> websocketContext)
        {
            try
            {
                var sendAsync = (WebSocketSendAsync) websocketContext["websocket.SendAsync"];
                var callCancelled = (CancellationToken) websocketContext["websocket.CallCancelled"];

                var memoryStream = new MemoryStream();
                var serializer = new JsonSerializer
                {
                    Converters = {new EtagJsonConverter()}
                };

				CreateWaitForClientCloseTask(websocketContext, callCancelled);

				while (callCancelled.IsCancellationRequested == false)
                {
                    var result = await manualResetEvent.WaitAsync(5000);
                    if (callCancelled.IsCancellationRequested)
                        break;

                    if (result == false)
                    {
                        await SendMessage(memoryStream, serializer,
                            new { Type = "Heartbeat", Time = SystemTime.UtcNow },
                            sendAsync, callCancelled);

                        if (lastMessageEnqueuedAndNotSent != null)
                        {
                            await SendMessage(memoryStream, serializer, lastMessageEnqueuedAndNotSent, sendAsync, callCancelled);
							lastMessageEnqueuedAndNotSent = null;
							lastMessageSentTick = Environment.TickCount;
                        }
                        continue;
                    }

                    manualResetEvent.Reset();

                    object message;
                    while (msgs.TryDequeue(out message))
                    {
						if (callCancelled.IsCancellationRequested)
							break;

                        if (CoolDownWithDataLossInMiliseconds > 0 && Environment.TickCount - lastMessageSentTick < CoolDownWithDataLossInMiliseconds)
                        {
                            lastMessageEnqueuedAndNotSent = message;
                            continue;
                        }

                        await SendMessage(memoryStream, serializer, message, sendAsync, callCancelled);
						lastMessageEnqueuedAndNotSent = null;
						lastMessageSentTick = Environment.TickCount;
                    }
                }
            }
            finally
            {
				OnDisconnection();
            }
        }

		private void CreateWaitForClientCloseTask(IDictionary<string, object> websocketContext, CancellationToken callCancelled)
	    {
			new Task(async () =>
			{
				var buffer = new ArraySegment<byte>(new byte[1024]);
				var receiveAsync = (WebSocketReceiveAsync)websocketContext["websocket.ReceiveAsync"];
				var closeAsync = (WebSocketCloseAsync) websocketContext["websocket.CloseAsync"];

				while (callCancelled.IsCancellationRequested == false)
				{
					try
					{
						WebSocketReceiveResult receiveResult = await receiveAsync(buffer, callCancelled);

						if (receiveResult.Item1 == WebSocketCloseMessageType)
						{
							var clientCloseStatus = (int) websocketContext["websocket.ClientCloseStatus"];
							var clientCloseDescription = (string) websocketContext["websocket.ClientCloseDescription"];

							if (clientCloseStatus == NormalClosureCode && clientCloseDescription == NormalClosureMessage)
							{
								await closeAsync(clientCloseStatus, clientCloseDescription, callCancelled);
							}

							//At this point the WebSocket is in a 'CloseReceived' state, so there is no need to continue waiting for messages
							break;
						}
					}
					catch (Exception e)
					{
						log.WarnException("Error when recieving message from web socket transport", e);
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

            var arraySegment = new ArraySegment<byte>(memoryStream.GetBuffer(), 0, (int) memoryStream.Position);
            await sendAsync(arraySegment, 1, true, callCancelled);
        }

        private void OnDisconnection()
        {
            Connected = false;
            Action onDisconnected = Disconnected;
            if (onDisconnected != null)
                onDisconnected();
        }

        protected async Task<IResource> GetActiveResource()
        {
            try
            {
                var localPath = _context.Request.Uri.LocalPath;
                var resourcePath = localPath.Substring(0, localPath.Length - ExpectedRequestSuffix.Length);

                var resourcePartsPathParts = resourcePath.Split('/');

                if (ExpectedRequestSuffix.Equals(localPath))
                {
                    ResourceName = "<system>";
                    return _options.SystemDatabase;
                }
                IResource activeResource;
                switch (resourcePartsPathParts[1])
                {
                    case COUNTERS_URL_PREFIX:
                        activeResource  = await _options.CountersLandlord.GetCounterInternal(resourcePath.Substring(COUNTERS_URL_PREFIX.Length + 2));
                        break;
                    case DATABASES_URL_PREFIX:
                        activeResource = await _options.DatabaseLandlord.GetDatabaseInternal(resourcePath.Substring(DATABASES_URL_PREFIX.Length + 2));
                        break;
                    case FILESYSTEMS_URL_PREFIX:
                        activeResource = await _options.FileSystemLandlord.GetFileSystemInternal(resourcePath.Substring(FILESYSTEMS_URL_PREFIX.Length + 2));
                        break;
                    default:
                        _context.Response.StatusCode = 400;
                        _context.Response.ReasonPhrase = "BadRequest";
                        _context.Response.Write("{'Error':'Illegal websocket path'}");
                        return null;
                }

                if (activeResource != null)
                    ResourceName = activeResource.Name;

                return activeResource;
            }
            catch (Exception e)
            {
                _context.Response.StatusCode = 500;
                _context.Response.ReasonPhrase = "InternalServerError";
                _context.Response.Write(e.ToString());
                return null;
            }
        }

        protected virtual async Task<bool> ValidateRequest()
        {
            if (string.IsNullOrEmpty(Id))
            {
                _context.Response.StatusCode = 400;
                _context.Response.ReasonPhrase = "BadRequest";
                _context.Response.Write("{ 'Error': 'Id is mandatory' }");
                return false;
            }

            ActiveTenant = await GetActiveResource();
            if (ActiveTenant== null)
            {
                return false;
            }

            return true;
        }

        protected virtual bool AuthenticateRequest(out IPrincipal user)
        {
            var singleUseToken = _context.Request.Query["singleUseAuthToken"];
            if (string.IsNullOrEmpty(singleUseToken) == false)
            {
                object msg;
                HttpStatusCode code;

                if (_options.MixedModeRequestAuthorizer.TryAuthorizeSingleUseAuthToken(singleUseToken, ResourceName, out msg, out code, out user) == false)
                {
                    _context.Response.StatusCode = (int)code;
                    _context.Response.ReasonPhrase = code.ToString();
                    _context.Response.Write(RavenJToken.FromObject(msg).ToString(Formatting.Indented));
                    return false;
                }
            }
            else
            {
                switch (_options.SystemDatabase.Configuration.AnonymousUserAccessMode)
                {
                    case AnonymousUserAccessMode.Admin:
                    case AnonymousUserAccessMode.All:
                    case AnonymousUserAccessMode.Get:
                        // this is effectively a GET request, so we'll allow it
                        // under this circumstances
                        user = CurrentOperationContext.User.Value;
                        break;
                    case AnonymousUserAccessMode.None:
                        _context.Response.StatusCode = 403;
                        _context.Response.ReasonPhrase = "Forbidden";
                        _context.Response.Write("{'Error': 'Single use token is required for authenticated web sockets connections' }");
                        user = null;
                        return false;
                    default:
                        throw new ArgumentOutOfRangeException(_options.SystemDatabase.Configuration.AnonymousUserAccessMode.ToString());
                }
            }
            return true;
        }

        public async Task<bool> TrySetupRequest()
        {
            IPrincipal user;
            if (!await ValidateRequest() || !AuthenticateRequest(out user))
            {
                return false;
            }
            
            RegisterTransportState();
            return true;
        }

        protected virtual void RegisterTransportState()
        {
            ActiveTenant.TransportState.Register(this);
        }
        

        private async Task<DocumentDatabase> GetDatabase()
        {
            var dbName = GetDatabaseName();

            if (dbName == null)
                return _options.SystemDatabase;

            DocumentDatabase documentDatabase;
            try
            {
                documentDatabase = await _options.DatabaseLandlord.GetDatabaseInternal(dbName);
            }
            catch (Exception e)
            {
                _context.Response.StatusCode = 500;
                _context.Response.ReasonPhrase = "InternalServerError";
                _context.Response.Write(e.ToString());
                return null;
            }
            return documentDatabase;
        }

		private async Task<RavenFileSystem> GetFileSystem()
		{
			var fsName = GetFileSystemName();

			if (fsName == null)
				return null;

			RavenFileSystem ravenFileSystem;
			try
			{
				ravenFileSystem = await _options.FileSystemLandlord.GetFileSystemInternal(fsName);
			}
			catch (Exception e)
			{
				_context.Response.StatusCode = 500;
				_context.Response.ReasonPhrase = "InternalServerError";
				_context.Response.Write(e.ToString());
				return null;
			}
			return ravenFileSystem;
		}

		private async Task<CounterStorage> GetCounterStorage()
		{
			var csName = GetCounterStorageName();

			if (csName == null)
				return null;

			CounterStorage counterStorage;
			try
			{
				counterStorage = await _options.CountersLandlord.GetCounterInternal(csName);
			}
			catch (Exception e)
			{
				_context.Response.StatusCode = 500;
				_context.Response.ReasonPhrase = "InternalServerError";
				_context.Response.Write(e.ToString());
				return null;
			}
			return counterStorage;
		}

        private string GetDatabaseName()
        {
            var localPath = _context.Request.Uri.LocalPath;
			const string databasesPrefix = "/databases/";

			return GetResourceName(localPath, databasesPrefix);
        }

        private string GetFileSystemName()
        {
            var localPath = _context.Request.Uri.LocalPath;
			const string fileSystemPrefix = "/fs/";

			return GetResourceName(localPath, fileSystemPrefix);
		}

		private string GetCounterStorageName()
		{
			var localPath = _context.Request.Uri.LocalPath;
			const string counterStoragePrefix = "/counters/";

			return GetResourceName(localPath, counterStoragePrefix);
		}

	    private string GetResourceName(string localPath, string prefix)
	    {
			if (localPath.StartsWith(prefix) == false)
				return null;

			var indexOf = localPath.IndexOf('/', prefix.Length + 1);

		    return (indexOf > -1) ? localPath.Substring(prefix.Length, indexOf - prefix.Length) : null;
	    }


        
    }

    public class WatchTrafficWebsocketTransport : WebSocketsTransport
    {
        public WatchTrafficWebsocketTransport(RavenDBOptions options, IOwinContext context)
            : base(options, context)
        {

        }

        override protected string ExpectedRequestSuffix
        {
            get { return WebSocketTransportFactory.WATCH_TRAFFIC_WEBSOCKET_SUFFIX; }
        }

        protected override bool AuthenticateRequest(out IPrincipal user)
        {
            if (!base.AuthenticateRequest(out user))
            {
                return false;
            }

            if (ResourceName == "<system>")
            {
                var oneTimetokenPrincipal = user as MixedModeRequestAuthorizer.OneTimetokenPrincipal;

                if ((oneTimetokenPrincipal == null || !oneTimetokenPrincipal.IsAdministratorInAnonymouseMode) &&
                    _options.SystemDatabase.Configuration.AnonymousUserAccessMode != AnonymousUserAccessMode.Admin)
                {
                    _context.Response.StatusCode = 403;
                    _context.Response.ReasonPhrase = "Forbidden";
                    _context.Response.Write("{'Error': 'Administrator user is required in order to trace the whole server' }");
                    return false;
                }
            }

            return true;
        }

        protected override void RegisterTransportState()
        {
            if (ResourceName != "<system>")
            {
                _options.RequestManager.RegisterResourceHttpTraceTransport(this, ResourceName);
            }
            else
            {
                _options.RequestManager.RegisterServerHttpTraceTransport(this);
            }
        }
    }

    public class CustomLogWebsocketTransport : WebSocketsTransport
    {
        public CustomLogWebsocketTransport(RavenDBOptions options, IOwinContext context)
            : base(options, context)
        {

        }

        override protected string ExpectedRequestSuffix
        {
            get { return WebSocketTransportFactory.CUSTOM_LOGGING_WEBSOCKET_SUFFIX; }
        }

        protected override async Task SendMessage(MemoryStream memoryStream, JsonSerializer serializer, object message, WebSocketSendAsync sendAsync, CancellationToken callCancelled)
        {
            var typedMessage = message as LogEventInfo;

            if (typedMessage != null)
            {
                message = new LogEventInfoFormatted(typedMessage);
            }

            base.SendMessage(memoryStream, serializer, message, sendAsync, callCancelled);
        }

        protected override async Task<bool> ValidateRequest()
        {
            if (!await base.ValidateRequest())
            {
                return false;
            }
            if (ResourceName != "<system>")
            {
                _context.Response.StatusCode = 400;
                _context.Response.ReasonPhrase = "BadRequest";
                _context.Response.Write("{ 'Error': 'Request should be withour resource context, or with system database' }");
                return false;
            }
            return true;
        }

        protected override bool AuthenticateRequest(out IPrincipal user)
        {
            if (!base.AuthenticateRequest(out user))
            {
                return false;
            }

            var oneTimetokenPrincipal = user as MixedModeRequestAuthorizer.OneTimetokenPrincipal;

            if ((oneTimetokenPrincipal == null || !oneTimetokenPrincipal.IsAdministratorInAnonymouseMode) &&
                _options.SystemDatabase.Configuration.AnonymousUserAccessMode != AnonymousUserAccessMode.Admin)
            {
                _context.Response.StatusCode = 403;
                _context.Response.ReasonPhrase = "Forbidden";
                _context.Response.Write("{'Error': 'Administrator user is required in order to trace the whole server' }");
                return false;
            }

            return true;
        }

        protected override void RegisterTransportState()
        {
            var logTarget = LogManager.GetTarget<OnDemandLogTarget>();
            logTarget.Register(this);
        }

    }
}