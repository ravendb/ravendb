using System.Collections.Generic;
using NLog.Layouts;
using Sparrow.Platform;

namespace Sparrow.Server.Global
{
    internal static class Constants
    {
        internal static class Encryption
        {
            public static readonly int XChachaAdLen = (int)Sodium.crypto_secretstream_xchacha20poly1305_abytes();
            public const int DefaultBufferSize = 4096;
        }

        internal class Logging
        {
            private Logging()
            {
            }

            internal static List<JsonAttribute> DefaultAdminLogsJsonAttributes = new()
            {
                new JsonAttribute("Date", "${longdate}"),
                new JsonAttribute("Level", "${level:uppercase=true}"),
                new JsonAttribute("Resource", "${event-properties:item=Resource}"),
                new JsonAttribute("Component", "${event-properties:item=Component}"),
                new JsonAttribute("Logger", "${logger}"),
                new JsonAttribute("Message", "${message:withexception=true}"),
                new JsonAttribute("Data", "${event-properties:item=Data}"),
            };
        }
    }
}
