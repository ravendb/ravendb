using System;

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

            internal const string DefaultHeaderAndFooterLayout = "Date|Level|ThreadID|Resource|Component|Logger|Message|Data";

            internal const string DefaultLayout = "${longdate:universalTime=true}|${level:uppercase=true}|${threadid}|${event-properties:item=Resource}|${event-properties:item=Component}|${logger}|${message:withexception=true}|${event-properties:item=Data}";

            // Server-only variants: they include the cluster NodeTag column via the ${rvn:...} layout renderer,
            // which is registered only in the Raven.Server assembly (see RavenLayoutRenderer.cs). Offline tools
            // (rvn, Voron.Recovery) must NOT use these - they have no node identity, and rvn does not register
            // the renderer, so parsing ${rvn:...} there would fail.
            internal const string DefaultServerHeaderAndFooterLayout = "Date|NodeTag|Level|ThreadID|Resource|Component|Logger|Message|Data";

            internal const string DefaultServerLayout = "${longdate:universalTime=true}|${rvn:NodeTag}|${level:uppercase=true}|${threadid}|${event-properties:item=Resource}|${event-properties:item=Component}|${logger}|${message:withexception=true}|${event-properties:item=Data}";

            internal class Properties
            {
                private Properties()
                {
                }

                internal const string Resource = "Resource";

                internal const string Component = "Component";

                internal const string Data = "Data";
            }

            internal class Names
            {
                private Names()
                {
                }

                internal const string ConsoleRuleName = "Raven_Console";

                internal const string PipeRuleName = "Raven_Pipe";

                internal const string AdminLogsRuleName = "Raven_WebSocket";

                internal const string MicrosoftRuleName = "Raven_Microsoft";

                internal const string SystemRuleName = "Raven_System";

                internal const string DefaultRuleName = "Raven_Default";

                internal const string DefaultAuditRuleName = "Raven_Default_Audit";
            }
        }

        internal static class Naming
        {
            public const string VectorPropertyName = "@vector";
            
            public static ReadOnlySpan<byte> VectorPropertyNameAsSpan => "@vector"u8;
        }
    }
}
