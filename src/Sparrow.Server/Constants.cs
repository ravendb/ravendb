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

            internal const string DefaultServerLayout = "${longdate:universalTime=true}|${rvn:NodeTag}|${level:uppercase=true}|${threadid}|${event-properties:item=Resource}|${event-properties:item=Component}|${logger}|${message:withexception=true}|${event-properties:item=Data}";

            internal static List<JsonAttribute> DefaultAdminLogsJsonAttributes = new()
            {
                new JsonAttribute("Date", "${longdate}"),
                new JsonAttribute("NodeTag", "${rvn:NodeTag}"),
                new JsonAttribute("Level", "${level:uppercase=true}"),
                new JsonAttribute("ThreadID", "${threadid}"),
                new JsonAttribute("Resource", "${event-properties:item=Resource}"),
                new JsonAttribute("Component", "${event-properties:item=Component}"),
                new JsonAttribute("Logger", "${logger}"),
                new JsonAttribute("Message", "${message:withexception=true}"),
                new JsonAttribute("Data", "${event-properties:item=Data}"),
            };
        }
    }
}
