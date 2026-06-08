using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.QueueSink;

/// <summary>
/// Helpers for encoding Azure Service Bus sources as strings stored in <see cref="QueueSinkScript.Queues"/>.
///
/// <para>Encoding convention:</para>
/// <list type="bullet">
///   <item><description><c>"queueName"</c> — a Service Bus queue.</description></item>
///   <item><description><c>"topicName;subscriptionName"</c> — a topic subscription.</description></item>
/// </list>
///
/// The semicolon is used as the separator because Service Bus naming rules forbid
/// <c>;</c> in queue, topic, and subscription names, so the delimiter is collision-safe.
/// </summary>
public static class AzureServiceBusSinkSource
{
    internal const char Separator = ';';

    /// <summary>
    /// Returns the encoded entry for a queue. Currently a pass-through; provided for symmetry and future-proofing.
    /// </summary>
    public static string Queue(string queueName)
    {
        if (string.IsNullOrWhiteSpace(queueName))
            throw new ArgumentException("Queue name must be non-empty.", nameof(queueName));

        if (queueName.IndexOf(Separator) >= 0)
            throw new ArgumentException($"Queue name must not contain the '{Separator}' character.", nameof(queueName));

        return queueName;
    }

    /// <summary>
    /// Returns the encoded entry for a topic subscription, in the form <c>topic;subscription</c>.
    /// </summary>
    public static string Subscription(string topicName, string subscriptionName)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentException("Topic name must be non-empty.", nameof(topicName));

        if (string.IsNullOrWhiteSpace(subscriptionName))
            throw new ArgumentException("Subscription name must be non-empty.", nameof(subscriptionName));

        if (topicName.IndexOf(Separator) >= 0)
            throw new ArgumentException($"Topic name must not contain the '{Separator}' character.", nameof(topicName));

        if (subscriptionName.IndexOf(Separator) >= 0)
            throw new ArgumentException($"Subscription name must not contain the '{Separator}' character.", nameof(subscriptionName));

        return $"{topicName}{Separator}{subscriptionName}";
    }

    internal static void ValidateScript(QueueSinkScript script, List<string> errors)
    {
        if (script?.Queues == null)
            return;

        foreach (var entry in script.Queues)
        {
            var error = ValidateEntry(entry);
            if (error == null)
                continue;

            errors.Add($"Script '{script.Name}': {error}");
        }
    }

    internal static string ValidateEntry(string entry)
    {
        if (string.IsNullOrWhiteSpace(entry))
            return "Azure Service Bus source entry cannot be empty.";

        // No separator => plain queue name, valid.
        // Otherwise TryParseSubscription enforces: exactly one separator, both halves non-empty.
        if (entry.IndexOf(Separator) >= 0 && TryParseSubscription(entry, out _, out _) == false)
            return $"Azure Service Bus subscription source '{entry}' is invalid. Use '<topic>{Separator}<subscription>' with a single '{Separator}' separator and both parts non-empty.";

        return null;
    }

    internal static bool TryParseSubscription(string entry, out string topic, out string subscription)
    {
        topic = null;
        subscription = null;

        if (entry == null)
            return false;

        // Split (no count) so any extra separator yields more than 2 parts and is rejected here.
        var parts = entry.Split(Separator);
        if (parts.Length != 2)
            return false;

        if (string.IsNullOrWhiteSpace(parts[0]) || string.IsNullOrWhiteSpace(parts[1]))
            return false;

        topic = parts[0];
        subscription = parts[1];
        return true;
    }
}
