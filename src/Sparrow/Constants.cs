using System.Collections.Generic;
using NLog.Layouts;

namespace Sparrow.Global
{
    internal static class Constants
    {
        internal static class Size
        {
            public const int Kilobyte = 1024;
            public const int Megabyte = 1024 * Kilobyte;
            public const int Gigabyte = 1024 * Megabyte;
            public const long Terabyte = 1024 * (long)Gigabyte;
        }

        internal class Logging
        {
            private Logging()
            {
            }

            internal const string DefaultHeaderAndFooterLayout = "Date|Level|Resource|Component|Logger|Message|Other";

            internal const string DefaultLayout = "${longdate}|${level:uppercase=true}|${event-properties:item=Resource}|${event-properties:item=Component}|${logger}|${message:withexception=true}|${event-properties:item=Other}";

            internal static List<JsonAttribute> DefaultAdminLogsJsonAttributes = new()
            {
                new JsonAttribute("Date", "${longdate}"),
                new JsonAttribute("Level", "${level:uppercase=true}"),
                new JsonAttribute("Resource", "${event-properties:item=Resource}"),
                new JsonAttribute("Component", "${event-properties:item=Component}"),
                new JsonAttribute("Logger", "${logger}"),
                new JsonAttribute("Message", "${message:withexception=true}"),
                new JsonAttribute("Other", "${event-properties:item=Other}"),
            };

            internal class Properties
            {
                private Properties()
                {
                }

                internal const string Resource = "Resource";

                internal const string Component = "Component";

                internal const string Other = "Other";
            }
        }
    }
}
