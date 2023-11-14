namespace Tests.Infrastructure;

public class RavenMultiplatformTheoryAttribute : RavenTheoryAttribute
{
    private readonly RavenPlatform _platform;
    private readonly RavenArchitecture _architecture;
    private readonly RavenIntrinsics _intrinsics;

    private string _skip;

   
    public RavenMultiplatformTheoryAttribute(RavenTestCategory category)
        : this(category, RavenPlatform.All)
    {
    }
    
    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenIntrinsics intrinsics)
        : this(category, RavenPlatform.All, RavenArchitecture.All, intrinsics)
    {
    }

    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenPlatform platform)
        : this(category, platform, RavenArchitecture.All, RavenIntrinsics.None)
    {
    }
    
    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenArchitecture architecture)
        : this(category, RavenPlatform.All, architecture, RavenIntrinsics.None)
    {
    }

    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenPlatform platform, RavenArchitecture architecture)
        : this(category, platform, architecture, RavenIntrinsics.None)
    {
    }
    
    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenPlatform platform, RavenArchitecture architecture, RavenIntrinsics intrinsics)
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

            return RavenMultiplatformFactAttribute.ShouldSkip(_platform, _architecture, _intrinsics, LicenseRequired, NightlyBuildOnly);
        }
        set => _skip = value;
    }
}
