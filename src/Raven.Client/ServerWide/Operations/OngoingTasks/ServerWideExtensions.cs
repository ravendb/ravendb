using System;
using System.Linq;

namespace Raven.Client.ServerWide.Operations.OngoingTasks
{
    internal static class ServerWideExtensions
    {
        internal static bool IsExcluded(this IServerWideTask task, string databaseName)
        {
            if (task.ExcludedDatabases == null)
                return false;

            return task.ExcludedDatabases.Contains(databaseName, StringComparer.OrdinalIgnoreCase);
        }
    }
}
