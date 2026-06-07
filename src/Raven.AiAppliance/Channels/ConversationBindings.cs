using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;

namespace Raven.AiAppliance.Channels;

/// <summary>
/// Get-or-create / resolve operations over <see cref="ConversationBinding"/>
/// docs — the service the embed chat uses to turn an opaque client token into
/// the hidden real conversation id (A2 fix, RavenDB-26700 auth follow-up).
///
/// Contract notes:
/// <list type="bullet">
/// <item><description><see cref="GetOrCreateAsync"/> returns an existing
/// binding's conversation id regardless of expiry — refresh-on-expired is a
/// future deterministic-key consumer's concern (no such consumer today;
/// Telegram derives ids directly).</description></item>
/// <item><description>L1 (security review): binding ids embed the bearer
/// token — NEVER emit a full binding id (or token) in exceptions or logs;
/// use <see cref="Describe"/> (public widgetId + 4-char key prefix).</description></item>
/// </list>
/// </summary>
internal sealed class ConversationBindings(IDocumentStore store, TimeProvider time)
{
    /// <summary>401 code: no binding doc for the presented token.</summary>
    internal const string UnknownCode = "conversation_unknown";

    /// <summary>401 code: binding exists but its validity window passed.</summary>
    internal const string ExpiredCode = "conversation_expired";

    /// <summary>
    /// Returns the conversation id bound to <paramref name="bindingId"/>,
    /// creating the binding (cluster-wide, atomic-guarded) when absent. The
    /// race-loser branch reads the winner's doc back — unreachable for random
    /// iFrame keys, real for deterministic keys (see the concurrency test).
    /// </summary>
    internal async Task<string> GetOrCreateAsync(
        string database,
        string bindingId,
        string widgetId,
        Func<string> newConversationId,
        TimeSpan ttl,
        CancellationToken ct)
    {
        // Fast path — only a deterministic-key retry (or race) can hit it;
        // random iFrame keys always miss and pay the cluster-wide write once.
        using (var session = store.OpenAsyncSession(database))
        {
            var existing = await session.LoadAsync<ConversationBinding>(bindingId, ct);
            if (existing is not null)
                return existing.ConversationId;
        }

        var now = time.GetUtcNow().UtcDateTime;
        var conversationId = newConversationId();

        try
        {
            using var session = store.OpenAsyncSession(new global::Raven.Client.Documents.Session.SessionOptions
            {
                Database = database,
                TransactionMode = TransactionMode.ClusterWide,
            });

            await session.StoreAsync(new ConversationBinding
            {
                Id = bindingId,
                ConversationId = conversationId,
                WidgetId = widgetId,
                CreatedAt = now,
                ExpiresAt = now + ttl,
            }, ct);

            await session.SaveChangesAsync(ct);
            return conversationId;
        }
        catch (ClusterTransactionConcurrencyException)
        {
            var winner = await ClusterWideRace.LoadWinnerAsync<ConversationBinding>(store, database, bindingId, ct);
            if (winner is null)
            {
                // L1: Describe(), never the raw binding id — it carries the token.
                throw new InvalidOperationException(
                    $"Conversation binding '{Describe(bindingId)}' never became visible after a cluster-tx conflict.");
            }

            return winner.ConversationId;
        }
    }

    /// <summary>
    /// Resolves a binding to its hidden conversation id with a standalone doc
    /// load. Returns <c>(id, null)</c> when live, or <c>(null, code)</c> with
    /// <see cref="UnknownCode"/> / <see cref="ExpiredCode"/>.
    /// </summary>
    internal async Task<(string? ConversationId, string? ErrorCode)> TryResolveAsync(
        string database, string bindingId, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        var binding = await session.LoadAsync<ConversationBinding>(bindingId, ct);
        return Validate(binding);
    }

    /// <summary>
    /// The single unknown/expired rule, decoupled from loading: the caller
    /// supplies the (possibly null) binding doc — loaded standalone by
    /// <see cref="TryResolveAsync"/>, or batched into another session's round
    /// trip (the embed chat path does this so a continuation turn pays ONE
    /// app-DB round trip for channel + binding — I1, impl review 2026-06-07).
    /// The embed chat maps both error codes onto 401.
    /// </summary>
    internal (string? ConversationId, string? ErrorCode) Validate(ConversationBinding? binding)
    {
        if (binding is null)
            return (null, UnknownCode);

        if (binding.ExpiresAt <= time.GetUtcNow().UtcDateTime)
            return (null, ExpiredCode);

        return (binding.ConversationId, null);
    }

    /// <summary>L1-safe rendering of a binding id: the public widgetId plus
    /// the first 4 chars of the (secret) key — enough to correlate, useless
    /// to replay. (The bound guard covers arbitrary deterministic keys a
    /// future consumer might use; iFrame keys are always 22 chars.)</summary>
    private static string Describe(string bindingId)
    {
        var keyStart = bindingId.LastIndexOf('/') + 1;
        var end = Math.Min(keyStart + 4, bindingId.Length);
        return $"{bindingId[..end]}…";
    }
}
