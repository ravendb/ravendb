using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using Sparrow.Logging;
using static Sparrow.Server.Platform.Posix.Syscall;

namespace Sparrow.Server.Platform.Posix;

public abstract class CGroup
{
    protected static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", nameof(CGroup));
    
    protected const string PROC_CGROUP_FILENAME = "/proc/self/cgroup";
    protected const string PROC_MOUNTINFO_FILENAME = "/proc/self/mountinfo";

    protected abstract string MemoryLimitFileName { get; } 
    protected abstract string MemoryUsageFileName { get; } 
    protected abstract string MaxMemoryUsageFileName { get; }

    private Lazy<CachedPath> _groupPathForMemory;
    
    private class CachedPath
    {
        public DateTime Time { get; private set; }
        public string Path { get;}

        public CachedPath(string path, DateTime time)
        {
            Path = path;
            Time = time;
        }

        public void Deprecate()
        {
            Time = DateTime.MinValue;
        }
    }

    protected CGroup()
    {
        _groupPathForMemory = CreateNewLazyCachedPath();
    }
    
    public long? GetMaxMemoryUsage() => ReadValue(GetGroupPathForMemory, MaxMemoryUsageFileName);
    public long? GetPhysicalMemoryUsage() => ReadValue(GetGroupPathForMemory, MemoryUsageFileName);
    public long? GetPhysicalMemoryLimit() => ReadValue(GetGroupPathForMemory, MemoryLimitFileName, CheckLimitValues);
    
    private Lazy<CachedPath> GetGroupPathForMemory()
    {
        var groupPathForMemory = _groupPathForMemory;
        if (DateTime.UtcNow - groupPathForMemory.Value.Time > TimeSpan.FromMinutes(1))
        {
            var newVal = CreateNewLazyCachedPath();
            Interlocked.CompareExchange(ref _groupPathForMemory, newVal, groupPathForMemory);
        }

        return _groupPathForMemory;
    }

    private Lazy<CachedPath> CreateNewLazyCachedPath()
    {
        return new Lazy<CachedPath>(() =>
        {
            string path = null;
            try
            {
                path = FindCGroupPathForMemory();
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Failed to get CGroup path for memory", e);
            }

            return new CachedPath(path, DateTime.UtcNow);
        });
    }
    
    private static bool CheckLimitValues(string textValue, out long? value)
    {
        //'max' stand for unlimited 
        if (textValue.StartsWith("max"))
        {
            value = long.MaxValue;
            return true;
        }

        value = null;
        return false;
    }

    private static long? ReadValue(Func<Lazy<CachedPath>> getBasePath, string file, CheckSpecialValues checkSpecialValues = null, bool retry = true)
    {
        var basePath = getBasePath();
        if (basePath.Value.Path == null)
            return null;

        try
        {
            var path = Path.Combine(basePath.Value.Path, file);
            return ReadMemoryValueFromFile(path, checkSpecialValues);
        }
        catch (Exception e)
        {
            if (e is not DirectoryNotFoundException || retry == false) 
                return null;
            
            //If the cgroup changed and the old cgroup was removed
            basePath.Value.Deprecate();
            return ReadValue(getBasePath, file, checkSpecialValues, false);
        }
    }

    protected abstract string FindCGroupPathForMemory();
    
    //51 34 0:46 / /sys/fs/cgroup/hugetlb rw,nosuid,nodev,noexec,relatime shared:27 - cgroup cgroup rw,hugetlb
    private static readonly Regex FindHierarchyMountReg = new Regex(@"^(?:\S+\s+){3}(?<mountroot>\S+)\s+(?<mountpath>\S+).* - (?<filesystemType>\S+)\s+\S+\s+(?:(?<options>[^,]+),?)+$", RegexOptions.Compiled);
    protected static IEnumerable<Match> FindHierarchyMount()
    {
        foreach (var line in File.ReadLines(PROC_MOUNTINFO_FILENAME))
        {
            var match = FindHierarchyMountReg.Match(line);
            if(match.Success == false)
                continue;

            if(match.Groups["filesystemType"].Value.StartsWith("cgroup") == false)
                continue;

            yield return match;
        }
    }
    
    protected static string CombinePaths(string mountRoot, string mountPath, string pathForSubsystem)
    {
        var toAppend = mountRoot.Length == 1 || pathForSubsystem.StartsWith(mountRoot)
            ? pathForSubsystem
            : pathForSubsystem[mountRoot.Length..];

        return mountPath + toAppend;
    }

    private delegate bool CheckSpecialValues(string textValue, out long? value);
    private static long? ReadMemoryValueFromFile(string fileName, CheckSpecialValues checkSpecialValues)
    {
        var txt = File.ReadAllText(fileName);
        if (checkSpecialValues != null && checkSpecialValues(txt, out var value))
            return value;
        var result = Convert.ToInt64(txt);
        if (result <= 0)
            return null;

        return result;
    }
}

public class CGroupV1 : CGroup
{
    protected override string MemoryLimitFileName => "memory.limit_in_bytes";
    protected override string MemoryUsageFileName => "memory.usage_in_bytes";
    protected override string MaxMemoryUsageFileName => "memory.max_usage_in_bytes";

    protected override string FindCGroupPathForMemory()
    {
        return FindCGroupPath(l => l.Contains("memory"));
    }
    
    private static string FindCGroupPath(Predicate<IEnumerable<string>> isSubSystem)
    {
        FindHierarchyMount(isSubSystem, out var mountRoot, out var mountPath);
        var pathForSubsystem = FindCGroupPathForSubsystem(isSubSystem);
        return CombinePaths(mountRoot, mountPath, pathForSubsystem);
    }
    private static void FindHierarchyMount(Predicate<IEnumerable<string>> isSubSystem, out string mountRoot, out string mountPath)
    {
        foreach (var match in FindHierarchyMount())
        {
            if(isSubSystem(match.Groups["options"].Captures.Select(x => x.Value)) == false)
                continue;

            mountRoot = match.Groups["mountroot"].Value;
            mountPath = match.Groups["mountpath"].Value;
            return;
        }

        throw new CGroupException($"Couldn't find hierarchy mount in {PROC_MOUNTINFO_FILENAME}");
    }
    
    // 8:memory:/user.slice/user-1000.slice/user@1000.service
    // 7:cpu,cpuacct:/user.slice
    private static readonly Regex FindCGroupPathForSubsystemReg = new Regex(@"^\d+:(?:(?<subsystem_list>[^,:]+),?)+:(?<path>.*)$", RegexOptions.Compiled);
    private static string FindCGroupPathForSubsystem(Predicate<IEnumerable<string>> isSubSystem)
    {
        foreach (var line in File.ReadLines(PROC_CGROUP_FILENAME))
        {
            var match = FindCGroupPathForSubsystemReg.Match(line);
            if(match.Success == false)
                throw new CGroupException($"Failed to parse cgroup info file contents - {line}.");
                
            if(isSubSystem(match.Groups["subsystem_list"].Captures.Select(x => x.Value)) == false)
                continue;

            return match.Groups["path"].Value;
        }

        return null;
    }
}

public class CGroupV2 : CGroup
{
    protected override string MemoryLimitFileName => "memory.max";
    protected override string MemoryUsageFileName => "memory.current";
    protected override string MaxMemoryUsageFileName => "memory.peak";

    protected override string FindCGroupPathForMemory() => FindCGroupPath();
    
    private static string FindCGroupPath()
    {
        FindHierarchyMount(out var mountRoot, out var mountPath);
        var pathForSubsystem = FindCGroupPathForSubsystem();
        
        return CombinePaths(mountRoot, mountPath, pathForSubsystem);
    }
    private static void FindHierarchyMount(out string mountRoot, out string mountPath)
    {
        foreach (var match in FindHierarchyMount())
        {
            mountRoot = match.Groups["mountroot"].Value;
            mountPath = match.Groups["mountpath"].Value;
            return;
        }

        throw new CGroupException($"Couldn't find hierarchy mount in {PROC_MOUNTINFO_FILENAME}");
    }
    
    // 0::/user.slice/user-1000.slice/user@1000.service/apps.slice/apps-org.gnome.Terminal.slice/vte-spawn-d7794050-ce4a-451b-92c2-a2433019409e.scope
    private static readonly Regex FindCGroupPathForSubsystemReg = new Regex(@"^\d+::(?<path>.*)$", RegexOptions.Compiled);
    private static string FindCGroupPathForSubsystem()
    {
        foreach (var line in File.ReadLines(PROC_CGROUP_FILENAME))
        {
            var match = FindCGroupPathForSubsystemReg.Match(line);
            if(match.Success == false)
                continue;
                
            return match.Groups["path"].Value;
        }

        return null;
    }
}

public class UnidentifiedCGroup : CGroup
{
    private readonly string _errorMessage;
    private DateTime _lastLog = DateTime.MinValue;
    protected override string MemoryLimitFileName => null;
    protected override string MemoryUsageFileName => null;
    protected override string MaxMemoryUsageFileName => null;

    public UnidentifiedCGroup(string errorMessage)
    {
        _errorMessage = errorMessage;
    }

    protected override string FindCGroupPathForMemory()
    {
        if (_lastLog + TimeSpan.FromMinutes(10) < DateTime.UtcNow)
        {
            _lastLog = DateTime.UtcNow;
            if(Logger.IsOperationsEnabled)
                Logger.Operations(_errorMessage);
        }

        return null;
    }
}

public static class CGroupHelper
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger("Server", nameof(CGroupHelper));
    public static readonly CGroup CGroup = GetCGroup();

    private static CGroup GetCGroup()
    {
        const uint TMPFS_MAGIC = 0x01021994;
        const uint CGROUP2_SUPER_MAGIC = 0x63677270;
        const string sysFsCgroupPath = "/sys/fs/cgroup";

        if (statfs(sysFsCgroupPath, out var stats) != 0)
            return new UnidentifiedCGroup($"Failed to get stats of {sysFsCgroupPath} because {Marshal.GetLastWin32Error()}");

        return stats.f_type switch
        {
            TMPFS_MAGIC => new CGroupV1(),
            CGROUP2_SUPER_MAGIC => new CGroupV2(),
            _ => new UnidentifiedCGroup($"Didn't identify CGroup - {nameof(stats.f_type)}:{stats.f_type}")
        };
    }
}

public class CGroupException : Exception
{
    public CGroupException(string message)
        :base(message)
    {
        
    }
} 


