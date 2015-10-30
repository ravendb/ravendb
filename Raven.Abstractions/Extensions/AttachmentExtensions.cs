using System;

namespace Raven.Abstractions.Extensions
{
    using System.IO;
    using System.Linq;

    using Raven.Abstractions.Data;
    using Raven.Json.Linq;

    [Obsolete("Use RavenFS instead.")]
    public static class AttachmentExtensions
    {
        public static bool IsConflictAttachment(this Attachment attachment)
        {
            var conflict = attachment.Metadata.Value<RavenJValue>(Constants.RavenReplicationConflict);
            if (conflict == null || conflict.Value<bool>() == false)
            {
                return false;
            }

            var keyParts = attachment.Key.Split('/');
            if (keyParts.Contains("conflicts") == false)
            {
                return false;
            }

            return true;
        } 
    }
}
