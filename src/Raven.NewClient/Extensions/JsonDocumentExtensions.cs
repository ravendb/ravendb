using Raven.NewClient.Client.Data;

namespace Raven.NewClient.Abstractions.Extensions
{
    using System.Linq;

    using Raven.NewClient.Abstractions.Data;
    using Raven.NewClient.Json.Linq;

    public static class JsonDocumentExtensions
    {
        public static bool IsConflictDocument(this JsonDocument document)
        {
            var conflict = document.Metadata.Value<RavenJValue>(Constants.Headers.RavenReplicationConflict);
            if (conflict == null || conflict.Value<bool>() == false)
            {
                return false;
            }

            var keyParts = document.Key.Split('/');
            if (keyParts.Contains("conflicts") == false)
            {
                return false;
            }

            var conflicts = document.DataAsJson.Value<RavenJArray>("Conflicts");
            if (conflicts != null)
            {
                return false;
            }

            return true;
        }
    }
}
