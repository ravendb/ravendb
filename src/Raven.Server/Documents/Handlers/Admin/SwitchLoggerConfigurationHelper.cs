using System;
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
            end = path.IndexOf('.', end);
            if (end < 0)
            {
                end = path.Length;
                break;
            }

            if (end - start == 0)
                throw new InvalidOperationException($"Double dot is not meaningful. pos {start} configuration path {path}");
            
            if (path[end - 1] == '\\')
            {
                end++;
                continue;
            }

            yield return GetSwitch();
            start = end += 1;
        }

        yield return GetSwitch();

        string GetSwitch()
        {
            string iterateSwitches = path.Substring(start, end - start);
            if (iterateSwitches.Contains('\\'))
                iterateSwitches = iterateSwitches.Replace("\\", "");
            return iterateSwitches;
        }
    }
    
    public static void GetConfigurationFromRoot(SwitchLogger root, Dictionary<string, LogMode> configuration)
    {
        if (root.IsModeOverrode)
            configuration.Add(root.Name, root.GetLogMode());
        GetConfiguration(root, configuration);
    }

    private static void GetConfiguration(SwitchLogger parent, Dictionary<string, LogMode> configuration)
    {
        foreach (var (_, child) in parent.Loggers)
        {
            var childMode = child.GetLogMode();
            if(child.IsModeOverrode && childMode != parent.GetLogMode())
                configuration.Add($"{child.Source}.{child.Name}", childMode);

            GetConfiguration(child, configuration);
        }
    }
}
