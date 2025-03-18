using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using static Sparrow.Server.Platform.Posix.Syscall;

namespace Sparrow.Server.Platform.Posix;

public abstract class CGroup
{
    protected static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForSparrowServer(typeof(CGroup));

    public const string PROC_SELF_CGROUP_FILENAME = "/proc/self/cgroup";
    public const string PROC_MOUNTINFO_FILENAME = "/proc/self/mountinfo";
    public const string PROC_CGROUPS_FILENAME = "/proc/cgroups";

    private const string MEMORY_CONTROLLER_NAME = "memory";

    protected abstract string MemoryLimitFileName { get; } 
    protected abstract string MemoryUsageFileName { get; } 
    protected abstract string MaxMemoryUsageFileName { get; }

    private Lazy<CachedPath> _groupPathForMemory;

    private sealed class CachedPath
    {
        public DateTime ExpiryTime { get; private set; }
        public string Path { get;}

        public CachedPath(string path, DateTime expiryTime)
        {
            Path = path;
            ExpiryTime = expiryTime;
        }

        public void Deprecate()
        {
            ExpiryTime = DateTime.MinValue;
        }
    }

    protected CGroup()
    {
        _groupPathForMemory = CreateNewLazyCachedPath();
    }

    public virtual long? GetMaxMemoryUsage()
    {
        try
        {
            return ReadValue(MaxMemoryUsageFileName);
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Failed to get CGroup max memory usage from {MaxMemoryUsageFileName} - {GetType().Name}", e);
            return null;
        }
    }
    public long? GetPhysicalMemoryUsage()
    {
        try
        {
            return ReadValue(MemoryUsageFileName);
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Failed to get CGroup current memory usage from {MemoryUsageFileName} - {GetType().Name}", e);
            return null;
        }
    }
    public long? GetPhysicalMemoryLimit()
    {
        try
        {
            return ReadValue(MemoryLimitFileName, CheckLimitValues);
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Failed to get CGroup memory limit from {MemoryLimitFileName} - {GetType().Name}", e);
            return null;
        }
    }
    
    private Lazy<CachedPath> GetGroupPathForMemory()
    {
        var groupPathForMemory = _groupPathForMemory;
        if (DateTime.UtcNow > groupPathForMemory.Value.ExpiryTime)
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
                if (IsControllerGroupsAvailable(MEMORY_CONTROLLER_NAME) == false)
                    return new CachedPath(null, DateTime.MaxValue);
                path = FindCGroupPathForMemory();
            }
            catch (Exception e)
            {
                if (Logger.IsWarnEnabled)
                    Logger.Warn("Failed to get CGroup path for memory", e);
            }

            return new CachedPath(path, DateTime.UtcNow + TimeSpan.FromMinutes(1));
        });
    }
    
    private static bool CheckLimitValues(string textValue, out long? value)
    {
        //'max' stands for unlimited 
        if (textValue.StartsWith("max"))
        {
            value = long.MaxValue;
            return true;
        }

        value = null;
        return false;
    }

    protected long? ReadValue(string file, CheckSpecialValues checkSpecialValues = null, bool retry = true)
    {
        var basePath = GetGroupPathForMemory();
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
                throw;
            
            //If the cgroup changed and the old cgroup was removed
            basePath.Value.Deprecate();
            return ReadValue(file, checkSpecialValues, false);
        }
    }

    private string FindCGroupPathForMemory()
    {
        return FindCGroupPathForMemoryInternal();
    }
    protected abstract string FindCGroupPathForMemoryInternal();
    
    //51 34 0:46 / /sys/fs/cgroup/hugetlb rw,nosuid,nodev,noexec,relatime shared:27 - cgroup cgroup rw,hugetlb
    private static readonly Regex FindHierarchyMountReg = new Regex(@"^(?:\S+\s+){3}(?<mountroot>\S+)\s+(?<mountpath>\S+).* - (?<filesystemType>\S+)\s+\S+\s+(?:(?<options>[^,]+),?)+$", RegexOptions.Compiled);
    protected static IEnumerable<Match> FindHierarchyMount()
    {
        foreach (var line in File.ReadLines(PROC_MOUNTINFO_FILENAME))
        {
            var match = FindHierarchyMountReg.Match(line);
            if (match.Success == false)
                continue;

            if (match.Groups["filesystemType"].Value.StartsWith("cgroup") == false)
                continue;

            yield return match;
        }
    }
    
    protected static string CombinePaths(string mountRoot, string mountPath, string pathForSubsystem)
    {
        var toAppend = mountRoot.Length == 1 || pathForSubsystem.StartsWith(mountRoot) == false
            ? pathForSubsystem
            : pathForSubsystem[mountRoot.Length..];

        return mountPath + toAppend;
    }

    //memory 0	205	1
    //cpu    2  232 1
    private static readonly Regex FindControllerGroupsAvailability = new Regex(@"^(?<subsys_name>[\w|_]+)\s+(?<hierarchy>\d+)\s+(?<num_cgroups>\d+)\s+(?<enabled>[1|0])$", RegexOptions.Compiled);
    private bool IsControllerGroupsAvailable(string subsysName)
    {
        foreach (string line in File.ReadLines(PROC_CGROUPS_FILENAME))
        {
            var match = FindControllerGroupsAvailability.Match(line);
            if (match.Success == false)
                continue;

            if (match.Groups["subsys_name"].Value.Equals(subsysName) == false)
                continue;

            return match.Groups["enabled"].Value == "1";
        }

        return false;
    }
    
    protected delegate bool CheckSpecialValues(string textValue, out long? value);
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

    #region for_test
    public bool ForTestIsControllerMemoryGroupsAvailable() => IsControllerGroupsAvailable(MEMORY_CONTROLLER_NAME);
    public string ForTestFindCGroupPathForMemory() => FindCGroupPathForMemoryInternal();
    #endregion
}

public sealed class CGroupV1 : CGroup
{
    protected override string MemoryLimitFileName => "memory.limit_in_bytes";
    protected override string MemoryUsageFileName => "memory.usage_in_bytes";
    protected override string MaxMemoryUsageFileName => "memory.max_usage_in_bytes";

    protected override string FindCGroupPathForMemoryInternal()
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
            if (isSubSystem(match.Groups["options"].Captures.Select(x => x.Value)) == false)
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
        foreach (var line in File.ReadLines(PROC_SELF_CGROUP_FILENAME))
        {
            var match = FindCGroupPathForSubsystemReg.Match(line);
            if (match.Success == false)
                throw new CGroupException($"Failed to parse cgroup info file contents - {line}.");
                
            if (isSubSystem(match.Groups["subsystem_list"].Captures.Select(x => x.Value)) == false)
                continue;

            return match.Groups["path"].Value;
        }

        return null;
    }
}

public sealed class CGroupV2 : CGroup
{
    private bool _hasMemoryPeakFile = true;

    protected override string MemoryLimitFileName => "memory.max";
    protected override string MemoryUsageFileName => "memory.current";
    protected override string MaxMemoryUsageFileName => "memory.peak";
    
    protected override string FindCGroupPathForMemoryInternal() => FindCGroupPath();
    
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
        foreach (var line in File.ReadLines(PROC_SELF_CGROUP_FILENAME))
        {
            var match = FindCGroupPathForSubsystemReg.Match(line);
            if (match.Success == false)
                continue;
                
            return match.Groups["path"].Value;
        }

        return null;
    }
    
    public override long? GetMaxMemoryUsage()
    {
        try
        {
            return _hasMemoryPeakFile ? ReadValue(MaxMemoryUsageFileName) : null;
        }
        catch (Exception e)
        {
            if (e is FileNotFoundException)
                _hasMemoryPeakFile = false;
            
            if (Logger.IsInfoEnabled)
                Logger.Info($"Failed to get CGroup max memory usage from {MaxMemoryUsageFileName} - {GetType().Name}", e);
            return null;
        }
    }
}

public sealed class UnidentifiedCGroup : CGroup
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

    protected override string FindCGroupPathForMemoryInternal()
    {
        if (_lastLog + TimeSpan.FromMinutes(10) < DateTime.UtcNow)
        {
            _lastLog = DateTime.UtcNow;
            if (Logger.IsInfoEnabled)
                Logger.Info(_errorMessage);
        }

        return null;
    }
}

public static class CGroupHelper
{
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

public sealed class CGroupException : Exception
{
    public CGroupException(string message)
        :base(message)
    {
        
    }
} 


