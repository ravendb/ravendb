using System.Net.WebSockets;
using Microsoft.Extensions.Options;
using Raven.Quill.Hosting;
using SlackNet;
using SlackNet.SocketMode;

namespace Raven.Quill.Slack;

internal sealed class SlackSdk
{
    internal const string HttpClientName = "slack";

    private const string DefaultApiBase = "https://slack.com/api/";

    private readonly IHttp _http;
    private readonly ISlackUrlBuilder _urlBuilder;

    public SlackSdk(IHttpClientFactory httpFactory, IOptions<ApplianceOptions> options)
    {
        Json = Default.JsonSettings();
        _http = Default.Http(Json, () => httpFactory.CreateClient(HttpClientName));
        _urlBuilder = new BaseUrlBuilder(Default.UrlBuilder(Json), options.Value.Slack.ApiUrl);
        Api = new SlackNet.SlackApiClient(_http, _urlBuilder, Json, token: "") { DisableRetryOnRateLimit = true };
    }

    public SlackJsonSettings Json { get; }

    public ISlackApiClient Api { get; }

    public ISlackSocketModeClient NewSocketClient(
        string appToken, IEventHandler handler, SlackNet.ILogger logger, ISocketObserver observer)
    {
        var services = new SlackServiceBuilder()
            .UseHttp(_ => _http)
            .UseJsonSettings(_ => Json)
            .UseUrlBuilder(_ => _urlBuilder)
            .UseLogger(_ => logger)
            .RegisterEventHandler(handler);

        var core = new CoreSocketModeClient(
            new SlackNet.SlackApiClient(_http, _urlBuilder, Json, appToken),
            new ObservedSockets(observer), Json, Default.Scheduler, logger);

        return new SlackSocketModeClient(
            core, Json, services.GetRequestListeners(), services.GetHandlerFactory(), logger);
    }

    internal interface ISocketObserver
    {
        void OnSocketOpened();

        void OnSocketClosed(Exception? error);
    }

    private sealed class ObservedSockets(ISocketObserver observer) : IWebSocketFactory
    {
        public IWebSocket Create(string uri) => new ObservedSocket(KeepAliveSockets.Instance.Create(uri), observer);
    }

    private sealed class ObservedSocket(IWebSocket inner, ISocketObserver observer) : IWebSocket
    {
        public WebSocketState State => inner.State;

        public Task Closed => inner.Closed;

        public IObservable<string> Messages => inner.Messages;

        public Task Send(string message) => inner.Send(message);

        public async Task<bool> Open(CancellationToken cancellationToken)
        {
            var opened = await inner.Open(cancellationToken);
            if (opened)
            {
                observer.OnSocketOpened();
                _ = ReportClosedAsync();
            }

            return opened;
        }

        public void Dispose() => inner.Dispose();

        private async Task ReportClosedAsync()
        {
            Exception? error;
            try
            {
                error = inner.Closed is Task<object> withResult ? await withResult as Exception : null;
                if (inner.Closed is not Task<object>)
                    await inner.Closed;
            }
            catch (Exception e)
            {
                error = e;
            }

            observer.OnSocketClosed(error);
        }
    }

    private sealed class BaseUrlBuilder(ISlackUrlBuilder inner, string apiUrl) : ISlackUrlBuilder
    {
        private readonly string _base = apiUrl.TrimEnd('/') + "/";

        public string Url(string apiMethod, Dictionary<string, object> args) =>
            _base + inner.Url(apiMethod, args)[DefaultApiBase.Length..];
    }

    private sealed class KeepAliveSockets : IWebSocketFactory
    {
        public static readonly KeepAliveSockets Instance = new();

        public IWebSocket Create(string uri)
        {
            var socket = new ClientWebSocket();
            socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(30);
            socket.Options.KeepAliveTimeout = TimeSpan.FromSeconds(20);
            return new WebSocketWrapper(socket, uri);
        }
    }
}
