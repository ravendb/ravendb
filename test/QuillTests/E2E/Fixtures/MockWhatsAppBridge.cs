using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace QuillTests.E2E.Fixtures;

public sealed class MockWhatsAppBridge : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly string _expectedToken;
    private readonly object _lock = new();

    private readonly Dictionary<string, SessionEntry> _sessions = new();
    private readonly List<(string Database, string ChannelId)> _started = [];
    private readonly List<(string Database, string ChannelId)> _restarted = [];
    private readonly List<(string Database, string ChannelId)> _deleted = [];
    private readonly List<SentMessage> _sent = [];
    private readonly List<(string Database, string ChannelId, string PhoneNumber)> _pairingPhoneNumbers = [];
    private int _nextMessageId = 1;

    public sealed record SentMessage(string Database, string ChannelId, string To, string Text);

    private sealed class SessionEntry
    {
        public string State = "pairing";
        public string? Qr;
        public string? PairingCode;
        public string? PhoneNumber;
        public string? LastError;
    }

    public string BaseAddress { get; }

    public bool Down { get; set; }

    private MockWhatsAppBridge(WebApplication app, string baseAddress, string expectedToken)
    {
        _app = app;
        BaseAddress = baseAddress;
        _expectedToken = expectedToken;
    }

    public IReadOnlyList<(string Database, string ChannelId)> StartedSessions
    {
        get { lock (_lock) return _started.ToArray(); }
    }

    public IReadOnlyList<(string Database, string ChannelId)> RestartedSessions
    {
        get { lock (_lock) return _restarted.ToArray(); }
    }

    public IReadOnlyList<(string Database, string ChannelId)> DeletedSessions
    {
        get { lock (_lock) return _deleted.ToArray(); }
    }

    public IReadOnlyList<SentMessage> SentMessages
    {
        get { lock (_lock) return _sent.ToArray(); }
    }

    public IReadOnlyList<(string Database, string ChannelId, string PhoneNumber)> PairingPhoneNumbers
    {
        get { lock (_lock) return _pairingPhoneNumbers.ToArray(); }
    }

    public bool HasSession(string database, string channelId)
    {
        lock (_lock) return _sessions.ContainsKey(KeyOf(database, channelId));
    }

    public void SetStatus(
        string database, string channelId, string state,
        string? qr = null, string? pairingCode = null, string? phoneNumber = null, string? lastError = null)
    {
        lock (_lock)
        {
            _sessions[KeyOf(database, channelId)] = new SessionEntry
            {
                State = state,
                Qr = qr,
                PairingCode = pairingCode,
                PhoneNumber = phoneNumber,
                LastError = lastError,
            };
        }
    }

    public void RemoveSession(string database, string channelId)
    {
        lock (_lock) _sessions.Remove(KeyOf(database, channelId));
    }

    public async Task WaitUntilAsync(Func<bool> condition, string what, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        while (condition() == false)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"MockWhatsAppBridge: timed out waiting for {what}");
            await Task.Delay(25);
        }
    }

    public void Reset()
    {
        lock (_lock)
        {
            _sessions.Clear();
            _started.Clear();
            _restarted.Clear();
            _deleted.Clear();
            _sent.Clear();
            _pairingPhoneNumbers.Clear();
        }

        Down = false;
    }

    public static async Task<MockWhatsAppBridge> StartAsync(string expectedToken)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        var app = builder.Build();

        MockWhatsAppBridge instance = null!;

        app.Use(async (ctx, next) =>
        {
            if (instance.Down)
            {
                ctx.Response.StatusCode = 503;
                await ctx.Response.WriteAsJsonAsync(new { error = "bridge down" });
                return;
            }

            if (ctx.Request.Headers["X-Quill-Bridge-Token"] != instance._expectedToken)
            {
                ctx.Response.StatusCode = 401;
                await ctx.Response.WriteAsJsonAsync(new { error = "invalid bridge token" });
                return;
            }

            await next();
        });

        app.MapPost("/sessions/{database}/{channelId}", async (string database, string channelId, HttpContext ctx) =>
            instance.HandleStart(database, channelId, await ReadPhoneNumberAsync(ctx)));

        app.MapGet("/sessions/{database}/{channelId}", (string database, string channelId) =>
            instance.HandleStatus(database, channelId));

        app.MapPost("/sessions/{database}/{channelId}/restart", async (string database, string channelId, HttpContext ctx) =>
            instance.HandleRestart(database, channelId, await ReadPhoneNumberAsync(ctx)));

        app.MapPost("/sessions/{database}/{channelId}/send", async (string database, string channelId, HttpContext ctx) =>
        {
            var body = await JsonNode.ParseAsync(ctx.Request.Body);
            return instance.HandleSend(database, channelId, body);
        });

        app.MapDelete("/sessions/{database}/{channelId}", (string database, string channelId) =>
            instance.HandleDelete(database, channelId));

        await app.StartAsync();

        var addresses = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
        var url = addresses?.Addresses.FirstOrDefault()
                  ?? throw new InvalidOperationException("MockWhatsAppBridge failed to bind a port.");

        instance = new MockWhatsAppBridge(app, url.TrimEnd('/'), expectedToken);
        return instance;
    }

    private IResult HandleStart(string database, string channelId, string? pairingPhoneNumber)
    {
        lock (_lock)
        {
            _started.Add((database, channelId));
            if (pairingPhoneNumber is not null)
                _pairingPhoneNumbers.Add((database, channelId, pairingPhoneNumber));

            var key = KeyOf(database, channelId);
            if (_sessions.ContainsKey(key) == false)
                _sessions[key] = NewPairingEntry(pairingPhoneNumber);

            return Results.Json(new { state = _sessions[key].State }, statusCode: 202);
        }
    }

    private static SessionEntry NewPairingEntry(string? pairingPhoneNumber) =>
        pairingPhoneNumber is null
            ? new SessionEntry { State = "pairing", Qr = $"QR-{Guid.NewGuid():N}" }
            : new SessionEntry { State = "pairing", PairingCode = "ABCD1234" };

    private static async Task<string?> ReadPhoneNumberAsync(HttpContext ctx)
    {
        try
        {
            var body = await JsonNode.ParseAsync(ctx.Request.Body);
            return (string?)body?["phoneNumber"];
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private IResult HandleStatus(string database, string channelId)
    {
        lock (_lock)
        {
            if (_sessions.TryGetValue(KeyOf(database, channelId), out var entry) == false)
                return Results.Json(new { error = "unknown session" }, statusCode: 404);

            return Results.Json(new
            {
                state = entry.State,
                qr = entry.Qr,
                qrExpiresAt = entry.Qr is null ? (DateTime?)null : DateTime.UtcNow.AddSeconds(60),
                pairingCode = entry.PairingCode,
                phoneNumber = entry.PhoneNumber,
                lastError = entry.LastError,
            });
        }
    }

    private IResult HandleRestart(string database, string channelId, string? pairingPhoneNumber)
    {
        lock (_lock)
        {
            _restarted.Add((database, channelId));
            if (pairingPhoneNumber is not null)
                _pairingPhoneNumbers.Add((database, channelId, pairingPhoneNumber));

            _sessions[KeyOf(database, channelId)] = NewPairingEntry(pairingPhoneNumber);
            return Results.Json(new { state = "pairing" }, statusCode: 202);
        }
    }

    private IResult HandleSend(string database, string channelId, JsonNode? body)
    {
        var to = (string?)body?["to"];
        var text = (string?)body?["text"];
        if (string.IsNullOrEmpty(to) || string.IsNullOrEmpty(text))
            return Results.Json(new { error = "to and text are required" }, statusCode: 400);

        lock (_lock)
        {
            if (_sessions.TryGetValue(KeyOf(database, channelId), out var entry) == false)
                return Results.Json(new { error = "unknown session" }, statusCode: 404);

            if (entry.State != "connected")
                return Results.Json(new { error = "session is not connected" }, statusCode: 409);

            _sent.Add(new SentMessage(database, channelId, to, text));
            return Results.Json(new { messageId = $"MOCK-{_nextMessageId++}" });
        }
    }

    private IResult HandleDelete(string database, string channelId)
    {
        lock (_lock)
        {
            _deleted.Add((database, channelId));
            _sessions.Remove(KeyOf(database, channelId));
            return Results.NoContent();
        }
    }

    private static string KeyOf(string database, string channelId) => $"{database}/{channelId}";

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
