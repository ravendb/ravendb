using Raven.Client.Documents.Conventions;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Metrics;
using Raven.Quill.Telegram;

namespace Raven.Quill.Infrastructure;

// Maps Quill's own documents to @-prefixed system collections so they group under Studio's
// system folder, out of the default collections view. Single source of truth: applied to the
// production stores in RavenStoreFactory and mirrored onto QuillTests' stores.
public static class QuillConventions
{
    public static string FindCollectionName(Type type) =>
        type == typeof(Channel) ? "@channels"
        : type == typeof(EmbedLink) ? "@embed-links"
        : type == typeof(TelegramLink) ? "@telegram-links"
        : type == typeof(TelegramBotReservation) ? "@telegram-bots"
        : type == typeof(ConversationPreview) ? ConversationPreview.Collection // "@ConversationPreviews"
        : type == typeof(AgentActionBindings) ? "@agent-actions"
        : DocumentConventions.DefaultGetCollectionName(type);
}
