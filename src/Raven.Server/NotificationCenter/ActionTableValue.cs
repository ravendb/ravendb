using System;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    public class ActionTableValue
    {
        public BlittableJsonReaderObject Json;

        public DateTime CreatedAt;

        public DateTime? PostponedUntil;
    }
}