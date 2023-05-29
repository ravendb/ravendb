using System.Collections.Generic;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Admin;

internal static class SwitchLoggerConfigurationHelper
{
    public static void ApplyConfiguration(SwitchLogger logger, IEnumerable<string> logPath, LogMode mode)
    {
        foreach (var switchName in logPath)
        {
            if (logger.Loggers.TryGet(switchName, out logger) == false)
                return;
        }
                        
        SetLoggerModeRecursively(logger, mode);
    }

    private static void SetLoggerModeRecursively(SwitchLogger root, LogMode mode)
    {
        root.SetLoggerMode(mode);
        foreach (var (_, child) in root.Loggers)
        {
            SetLoggerModeRecursively(child, mode);
        }
    }
        
    public static IEnumerable<string> IterateSwitches(string path)
    {
        int start = 0;
        var end = 0;
        while (end < path.Length)
        {
            end = path.IndexOf('.', start);
            if (end < 0)
                break;

            if (end != 0 && path[end - 1] == '\\')
                continue;

            string iterateSwitches = path.Substring(start, end - start);
            yield return iterateSwitches;
            start = end + 1;
        }

        yield return path.Substring(start, path.Length - start);
    }
}
