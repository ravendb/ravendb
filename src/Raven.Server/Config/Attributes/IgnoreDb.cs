using System;
using System.Collections.Generic;

namespace Raven.Server.Config.Attributes
{
    public class IgnoreDb
    {
        public static readonly HashSet<string> Urls = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            // allow to get files not secret if you have access to the server (not for the specific DB)
            "/databases",
            "/fs",
            "/license/status",
            "/studio-tasks/server-configs"
        };
    }
}
