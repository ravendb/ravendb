using System;
using System.Runtime.InteropServices;
using Xunit;

namespace Tests.Infrastructure;

[Flags]
public enum RavenPlatform : byte
{
    Windows = 1 << 1,
    Linux = 1 << 2,
    OsX = 1 << 3,
    All = Windows | Linux | OsX
}

[Flags]
public enum RavenArchitecture
{
    Arm = 1 << 1,
    Arm64 = 1 << 2,
    X64 = 1 << 3,
    X86 = 1 << 4,
    All = Arm | Arm64 | X64 | X86
}

public class MultiplatformFactAttribute : FactAttribute
{
    private readonly RavenPlatform _platform;
    private readonly RavenArchitecture _architecture;

    public MultiplatformFactAttribute(RavenPlatform platform = RavenPlatform.All)
     : this(platform, RavenArchitecture.All)
    {
    }

    public MultiplatformFactAttribute(RavenArchitecture architecture = RavenArchitecture.All)
        : this(RavenPlatform.All, architecture)
    {
    }

    public MultiplatformFactAttribute(RavenPlatform platform = RavenPlatform.All, RavenArchitecture architecture = RavenArchitecture.All)
    {
        _platform = platform;
        _architecture = architecture;
    }

    public override string Skip => ShouldSkip(_platform, _architecture);

    internal static string ShouldSkip(RavenPlatform platform, RavenArchitecture architecture)
    {
        var matchesPlatform = Match(platform);
        var matchesArchitecture = Match(architecture);

        if (matchesPlatform == false || matchesArchitecture == false)
            return $"Test can be run only on '{platform}' ({architecture})";

        return null;
    }

    private static bool Match(RavenPlatform platform)
    {
        if (platform == RavenPlatform.All)
            return true;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && platform.HasFlag(RavenPlatform.Windows))
            return true;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && platform.HasFlag(RavenPlatform.Linux))
            return true;

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX) && platform.HasFlag(RavenPlatform.OsX))
            return true;

        return false;
    }

    private static bool Match(RavenArchitecture architecture)
    {
        if (architecture == RavenArchitecture.All)
            return true;

        switch (RuntimeInformation.ProcessArchitecture)
        {
            case Architecture.X86:
                return architecture.HasFlag(RavenArchitecture.X86);
            case Architecture.X64:
                return architecture.HasFlag(RavenArchitecture.X64);
            case Architecture.Arm:
                return architecture.HasFlag(RavenArchitecture.Arm);
            case Architecture.Arm64:
                return architecture.HasFlag(RavenArchitecture.Arm64);
            default:
                throw new ArgumentOutOfRangeException(nameof(architecture), architecture, $"Invalid architecture ({architecture})");
        }
    }
}
