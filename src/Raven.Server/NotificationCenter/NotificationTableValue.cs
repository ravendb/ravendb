using System;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    public class NotificationTableValue
    {
        public BlittableJsonReaderObject Json;

        public DateTime CreatedAt;

        public DateTime? PostponedUntil;
    }
}