using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Quill.WhatsApp;

internal sealed class WhatsAppBridgeClient(HttpClient http, IWhatsAppBridgeSecret secret) : IWhatsAppBridgeClient
{
    private const string TokenHeader = "X-Quill-Bridge-Token";

    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
    };

    public async Task StartSessionAsync(string database, string channelId, string? pairingPhoneNumber, CancellationToken ct)
    {
        using var content = PairingContent(pairingPhoneNumber);
        using var response = await SendAsync(HttpMethod.Post, SessionPath(database, channelId), content, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<WhatsAppSessionStatus?> GetSessionStatusAsync(string database, string channelId, CancellationToken ct)
    {
        using var response = await SendAsync(HttpMethod.Get, SessionPath(database, channelId), content: null, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
            return null;

        await EnsureSuccessAsync(response, ct);
        return await ReadStatusAsync(response, ct);
    }

    public async Task RestartSessionAsync(string database, string channelId, string? pairingPhoneNumber, CancellationToken ct)
    {
        using var content = PairingContent(pairingPhoneNumber);
        using var response = await SendAsync(HttpMethod.Post, SessionPath(database, channelId) + "/restart", content, ct);
        await EnsureSuccessAsync(response, ct);
    }

    private static HttpContent? PairingContent(string? pairingPhoneNumber) =>
        string.IsNullOrWhiteSpace(pairingPhoneNumber)
            ? null
            : JsonContent.Create(new { phoneNumber = pairingPhoneNumber }, options: SerializerOptions);

    public async Task SendTextAsync(string database, string channelId, string toJid, string text, CancellationToken ct)
    {
        using var content = JsonContent.Create(new { to = toJid, text }, options: SerializerOptions);
        using var response = await SendAsync(HttpMethod.Post, SessionPath(database, channelId) + "/send", content, ct);
        if (response.StatusCode == HttpStatusCode.Conflict)
            throw new WhatsAppSendConflictException();

        await EnsureSuccessAsync(response, ct);
    }

    public async Task DeleteSessionAsync(string database, string channelId, CancellationToken ct)
    {
        using var response = await SendAsync(HttpMethod.Delete, SessionPath(database, channelId), content: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    private async Task<HttpResponseMessage> SendAsync(HttpMethod method, string path, HttpContent? content, CancellationToken ct)
    {
        var token = await secret.GetAsync(ct)
            ?? throw new WhatsAppBridgeException("whatsapp bridge token is not available");

        using var request = new HttpRequestMessage(method, path);
        request.Headers.Add(TokenHeader, token);
        request.Content = content;

        try
        {
            return await http.SendAsync(request, ct);
        }
        catch (HttpRequestException e)
        {
            throw new WhatsAppBridgeException($"whatsapp bridge is unreachable: {e.Message}", e);
        }
        catch (OperationCanceledException e) when (ct.IsCancellationRequested == false)
        {
            throw new WhatsAppBridgeException("whatsapp bridge did not respond in time", e);
        }
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.IsSuccessStatusCode)
            return;

        var body = await response.Content.ReadAsStringAsync(ct);
        throw new WhatsAppBridgeException(
            $"whatsapp bridge responded {(int)response.StatusCode}: {Truncate(body)}");
    }

    private static async Task<WhatsAppSessionStatus> ReadStatusAsync(HttpResponseMessage response, CancellationToken ct)
    {
        try
        {
            var status = await response.Content.ReadFromJsonAsync<WhatsAppSessionStatus>(SerializerOptions, ct);
            return status ?? throw new WhatsAppBridgeException("whatsapp bridge returned an empty status");
        }
        catch (JsonException e)
        {
            throw new WhatsAppBridgeException($"whatsapp bridge returned malformed status: {e.Message}", e);
        }
    }

    private static string SessionPath(string database, string channelId) =>
        $"/sessions/{Uri.EscapeDataString(database)}/{Uri.EscapeDataString(channelId)}";

    private static string Truncate(string body) =>
        body.Length <= 200 ? body : body[..200];
}
