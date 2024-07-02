using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using Sparrow;

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

[Flags]
public enum RavenIntrinsics
{
    None = 0,
    ArmBase = 1 << 1,
    Arm64 = 1 << 2,
    Sse = 1 << 3,
    Avx256 = 1 << 4,
    Avx512 = 1 << 5,

    Vector128 = 1 << 7,
    Vector256 = 1 << 8,
    Vector512 = 1 << 9
}

public class RavenMultiplatformFactAttribute : RavenFactAttribute
{
    private static readonly bool ForceUsing32BitsPager;

    private readonly RavenPlatform _platform;
    private readonly RavenArchitecture _architecture;
    private readonly RavenIntrinsics _intrinsics;

    private string _skip;

    static RavenMultiplatformFactAttribute()
    {
        if (bool.TryParse(Environment.GetEnvironmentVariable("VORON_INTERNAL_ForceUsing32BitsPager"), out var result))
            ForceUsing32BitsPager = result;
    }

    public RavenMultiplatformFactAttribute(RavenTestCategory category, RavenPlatform platform = RavenPlatform.All)
     : this(category, platform, RavenArchitecture.All)
    {
    }

    public RavenMultiplatformFactAttribute(RavenTestCategory category, RavenArchitecture architecture = RavenArchitecture.All)
        : this(category, RavenPlatform.All, architecture)
    {
    }

    public RavenMultiplatformFactAttribute(RavenTestCategory category, RavenIntrinsics intrinsics = RavenIntrinsics.None)
        : this(category, RavenPlatform.All, RavenArchitecture.All, intrinsics)
    {
    }

    public RavenMultiplatformFactAttribute(RavenTestCategory category, RavenPlatform platform = RavenPlatform.All, RavenArchitecture architecture = RavenArchitecture.All, RavenIntrinsics intrinsics = RavenIntrinsics.None)
        : base(category)
    {
        _platform = platform;
        _architecture = architecture;
        _intrinsics = intrinsics;
    }

    public bool NightlyBuildOnly { get; set; }

    public override string Skip
    {
        get
        {
            var skip = _skip;
            if (skip != null)
                return skip;

            return ShouldSkip(_platform, _architecture, _intrinsics, LicenseRequired, NightlyBuildOnly);
        }
        set => _skip = value;
    }

    internal static string ShouldSkip(RavenPlatform platform, RavenArchitecture architecture, RavenIntrinsics intrinsics, bool licenseRequired, bool nightlyBuildOnly)
    {
        if (licenseRequired && LicenseRequiredFactAttribute.ShouldSkip())
            return LicenseRequiredFactAttribute.SkipMessage;

        if (nightlyBuildOnly && NightlyBuildTheoryAttribute.IsNightlyBuild == false)
            return NightlyBuildTheoryAttribute.SkipMessage;

        var matchesPlatform = Match(platform);
        var matchesArchitecture = Match(architecture);
        var matchesIntrinsics = Match(intrinsics);

        if (matchesPlatform == false || matchesArchitecture == false || matchesIntrinsics == false)
        {
            var message = $"Test can be run only on '{platform}' ({architecture})";
            if (matchesIntrinsics == false)
            {
                return message + $" with supported intrinsics: '{string.Join(", ", RequiredIntrinsics())}'";

                IEnumerable<string> RequiredIntrinsics()
                {
                    foreach (var flag in Enum.GetValues<RavenIntrinsics>())
                    {
                        if (flag is RavenIntrinsics.None)
                            continue;

                        if (intrinsics.HasFlag(flag))
                            yield return intrinsics.ToString();
                    }
                }
            }

            return message;
        }

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

    private static bool Match(RavenIntrinsics intrinsics)
    {
        if (intrinsics is RavenIntrinsics.None)
            return true;

        if (intrinsics.HasFlag(RavenIntrinsics.Vector128) && AdvInstructionSet.IsAcceleratedVector128 == false)
            return false;

        if (intrinsics.HasFlag(RavenIntrinsics.Vector256) && AdvInstructionSet.IsAcceleratedVector256 == false)
            return false;

        if (intrinsics.HasFlag(RavenIntrinsics.Vector512) && AdvInstructionSet.IsAcceleratedVector512 == false)
            return false;

        if (intrinsics.HasFlag(RavenIntrinsics.ArmBase) && AdvInstructionSet.Arm.IsSupported == false)
            return false;

        if (intrinsics.HasFlag(RavenIntrinsics.Arm64) && AdvInstructionSet.Arm.IsSupportedArm64 == false)
            return false;

        if (intrinsics.HasFlag(RavenIntrinsics.Sse) && AdvInstructionSet.X86.IsSupportedSse == false)
            return false;

        if (intrinsics.HasFlag(RavenIntrinsics.Avx256) && AdvInstructionSet.X86.IsSupportedAvx256 == false)
            return false;

        if (intrinsics.HasFlag(RavenIntrinsics.Avx512) && AdvInstructionSet.X86.IsSupportedAvx512 == false)
            return false;

        return true;
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
