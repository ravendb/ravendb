using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Text.Json.Nodes;

namespace QuillTests.E2E.Fixtures;

public sealed class MockSlackApi : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly object _lock = new();

    private readonly Dictionary<string, BotEntry> _bots = new();
    private readonly Dictionary<string, string?> _userEmails = new();
    private readonly List<string> _userInfoCalls = [];
    private readonly List<SentMessage> _sent = [];
    private readonly List<EditedMessage> _edited = [];
    private readonly List<string> _authTestCalls = [];
    private int _nextTs = 1;

    public sealed record SentMessage(string BotToken, string Channel, string Text, string Ts, string? Parse);

    public sealed record EditedMessage(string BotToken, string Channel, string Ts, string Text, string? Parse);

    private sealed record BotEntry(string TeamId, string TeamName, string BotUserId, string BotId);

    public string BaseAddress { get; }

    public bool Down { get; set; }

    public string? SendError { get; set; }

    public bool NextUpdateRateLimit429 { get; set; }

    public bool UsersReadScopeGranted { get; set; } = true;

    private MockSlackApi(WebApplication app, string baseAddress)
    {
        _app = app;
        BaseAddress = baseAddress;
    }

    public IReadOnlyList<SentMessage> SentMessages
    {
        get { lock (_lock) return _sent.ToArray(); }
    }

    public IReadOnlyList<EditedMessage> EditedMessages
    {
        get { lock (_lock) return _edited.ToArray(); }
    }

    public IReadOnlyList<string> AuthTestCalls
    {
        get { lock (_lock) return _authTestCalls.ToArray(); }
    }

    public IReadOnlyList<string> UserInfoCalls
    {
        get { lock (_lock) return _userInfoCalls.ToArray(); }
    }

    // a user known to Slack but with no email on their profile is registered with a null email
    public void AddUser(string userId, string? email)
    {
        lock (_lock)
            _userEmails[userId] = email;
    }

    public void AddBot(string botToken, string teamId, string teamName, string botUserId, string botId = "B0MOCK")
    {
        lock (_lock)
            _bots[botToken] = new BotEntry(teamId, teamName, botUserId, botId);
    }

    public void Reset()
    {
        lock (_lock)
        {
            _bots.Clear();
            _sent.Clear();
            _edited.Clear();
            _authTestCalls.Clear();
            _userEmails.Clear();
            _userInfoCalls.Clear();
        }
        Down = false;
        SendError = null;
        NextUpdateRateLimit429 = false;
        UsersReadScopeGranted = true;
    }

    public Task WaitUntilAsync(Func<bool> condition, string what, TimeSpan? timeout = null) =>
        MockApiWait.UntilAsync(nameof(MockSlackApi), condition, what, timeout);

    public Task WaitUntilAsync(Func<Task<bool>> condition, string what, TimeSpan? timeout = null) =>
        MockApiWait.UntilAsync(nameof(MockSlackApi), condition, what, timeout);

    public static async Task<MockSlackApi> StartAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        var app = builder.Build();

        MockSlackApi instance = null!;

        app.Use(async (ctx, next) =>
        {
            if (instance.Down)
            {
                ctx.Response.StatusCode = 503;
                return;
            }
            await next(ctx);
        });

        app.MapPost("/auth.test", (HttpContext ctx) => instance.HandleAuthTest(ctx));

        app.MapGet("/users.info", (HttpContext ctx) => instance.HandleUserInfo(ctx));

        app.MapPost("/chat.postMessage", async (HttpContext ctx) =>
        {
            var body = await JsonNode.ParseAsync(ctx.Request.Body);
            return instance.HandlePostMessage(ctx, body);
        });

        app.MapPost("/chat.update", async (HttpContext ctx) =>
        {
            var body = await JsonNode.ParseAsync(ctx.Request.Body);
            return instance.HandleUpdate(ctx, body);
        });

        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var url = addresses?.Addresses.FirstOrDefault()
                  ?? throw new InvalidOperationException("MockSlackApi failed to bind a port.");

        instance = new MockSlackApi(app, url.TrimEnd('/'));
        return instance;
    }

    private IResult HandleAuthTest(HttpContext ctx)
    {
        var token = BearerToken(ctx);
        BotEntry? entry;
        lock (_lock)
        {
            _authTestCalls.Add(token);
            entry = _bots.GetValueOrDefault(token);
        }

        if (entry is null)
            return SlackError("invalid_auth");

        return Results.Json(new JsonObject
        {
            ["ok"] = true,
            ["url"] = $"https://{entry.TeamName.ToLowerInvariant()}.slack.com/",
            ["team"] = entry.TeamName,
            ["user"] = "quill-bot",
            ["team_id"] = entry.TeamId,
            ["user_id"] = entry.BotUserId,
            ["bot_id"] = entry.BotId,
        });
    }

    private IResult HandleUserInfo(HttpContext ctx)
    {
        BotEntry? entry;
        lock (_lock)
            entry = _bots.GetValueOrDefault(BearerToken(ctx));

        if (entry is null)
            return SlackError("invalid_auth");

        if (UsersReadScopeGranted == false)
            return SlackError("missing_scope");

        var userId = ctx.Request.Query["user"].ToString();

        bool known;
        string? email;
        lock (_lock)
        {
            _userInfoCalls.Add(userId);
            known = _userEmails.TryGetValue(userId, out email);
        }

        if (known == false)
            return SlackError("user_not_found");

        var profile = new JsonObject();
        if (email is not null)
            profile["email"] = email;

        return Results.Json(new JsonObject
        {
            ["ok"] = true,
            ["user"] = new JsonObject
            {
                ["id"] = userId,
                ["team_id"] = entry.TeamId,
                ["profile"] = profile,
            },
        });
    }

    private IResult HandlePostMessage(HttpContext ctx, JsonNode? body)
    {
        BotEntry? entry;
        lock (_lock)
            entry = _bots.GetValueOrDefault(BearerToken(ctx));

        if (entry is null)
            return SlackError("invalid_auth");

        if (SendError is { } error)
            return SlackError(error);

        var channel = body?["channel"]?.GetValue<string>() ?? "";
        var text = body?["text"]?.GetValue<string>() ?? "";
        var parse = body?["parse"]?.GetValue<string>();

        string ts;
        lock (_lock)
        {
            ts = string.Create(CultureInfo.InvariantCulture, $"1700000000.{_nextTs++:D6}");
            _sent.Add(new SentMessage(BearerToken(ctx), channel, text, ts, parse));
        }

        return Results.Json(new JsonObject { ["ok"] = true, ["channel"] = channel, ["ts"] = ts });
    }

    private IResult HandleUpdate(HttpContext ctx, JsonNode? body)
    {
        BotEntry? entry;
        lock (_lock)
            entry = _bots.GetValueOrDefault(BearerToken(ctx));

        if (entry is null)
            return SlackError("invalid_auth");

        if (NextUpdateRateLimit429)
        {
            NextUpdateRateLimit429 = false;
            ctx.Response.Headers.RetryAfter = "1";
            return Results.Json(new JsonObject { ["ok"] = false, ["error"] = "ratelimited" }, statusCode: 429);
        }

        if (SendError is { } error)
            return SlackError(error);

        var channel = body?["channel"]?.GetValue<string>() ?? "";
        var ts = body?["ts"]?.GetValue<string>() ?? "";
        var text = body?["text"]?.GetValue<string>() ?? "";
        var parse = body?["parse"]?.GetValue<string>();

        lock (_lock)
        {
            if (_sent.Any(m => m.Ts == ts) == false)
                return SlackError("message_not_found");

            _edited.Add(new EditedMessage(BearerToken(ctx), channel, ts, text, parse));
        }

        return Results.Json(new JsonObject { ["ok"] = true, ["channel"] = channel, ["ts"] = ts });
    }

    private static string BearerToken(HttpContext ctx)
    {
        var header = ctx.Request.Headers.Authorization.ToString();
        return header.StartsWith("Bearer ", StringComparison.Ordinal) ? header["Bearer ".Length..] : "";
    }

    private static IResult SlackError(string error) =>
        Results.Json(new JsonObject { ["ok"] = false, ["error"] = error });

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
