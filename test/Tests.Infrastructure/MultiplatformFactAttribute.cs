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
    AllArm = Arm | Arm64,
    AllX64 = Arm64 | X64,
    AllX86 = Arm | X86,
    All = AllX64 | AllX86
}

public class MultiplatformFactAttribute : FactAttribute
{
    private static readonly bool ForceUsing32BitsPager;

    private readonly RavenPlatform _platform;
    private readonly RavenArchitecture _architecture;

    private string _skip;

    static MultiplatformFactAttribute()
    {
        if (bool.TryParse(Environment.GetEnvironmentVariable("VORON_INTERNAL_ForceUsing32BitsPager"), out var result))
            ForceUsing32BitsPager = result;
    }

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

    public bool LicenseRequired { get; set; }

    public override string Skip
    {
        get
        {
            var skip = _skip;
            if (skip != null)
                return skip;

            return ShouldSkip(_platform, _architecture, LicenseRequired);
        }
        set => _skip = value;
    }

    internal static string ShouldSkip(RavenPlatform platform, RavenArchitecture architecture, bool licenseRequired)
    {
        if (licenseRequired && LicenseRequiredFactAttribute.ShouldSkip(licenseRequired: true))
            return LicenseRequiredFactAttribute.SkipMessage;

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

        if (ForceUsing32BitsPager)
            return architecture.HasFlag(RavenArchitecture.Arm) || architecture.HasFlag(RavenArchitecture.X86);

        return RuntimeInformation.ProcessArchitecture switch
        {
            Architecture.X86 => architecture.HasFlag(RavenArchitecture.X86),
            Architecture.X64 => architecture.HasFlag(RavenArchitecture.X64),
            Architecture.Arm => architecture.HasFlag(RavenArchitecture.Arm),
            Architecture.Arm64 => architecture.HasFlag(RavenArchitecture.Arm64),
            _ => throw new ArgumentOutOfRangeException(nameof(architecture), architecture, $"Invalid architecture ({architecture})")
        };
    }
}
