using System;
using PgSqlParser;

namespace Raven.Server.Integrations.PostgreSQL
{
    public static class PgSqlParserNative
    {
        private static readonly Lazy<bool> Available = new(Probe);
        
        public static bool IsAvailable => Available.Value;

        private static bool Probe()
        {
            try
            {
                _ = Parser.Parse("SELECT 1");
                return true;
            }
            catch (Exception e) when (IsNativeLoadFailure(e))
            {
                return false;
            }
        }

        private static bool IsNativeLoadFailure(Exception e)
        {
            for (var current = e; current != null; current = current.InnerException)
            {
                if (current is DllNotFoundException or BadImageFormatException)
                    return true;
            }

            return false;
        }
    }
}
