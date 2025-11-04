using System;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    public sealed class NotificationTableValue : IDisposable
    {
        public BlittableJsonReaderObject Json;

        public DateTime CreatedAt;

        public DateTime? PostponedUntil;

        public long Type;
        
        public long Reason;

        public void Dispose()
        {
            Json?.Dispose();
        }
    }
}
