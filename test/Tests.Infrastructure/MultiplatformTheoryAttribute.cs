using Xunit;

namespace Tests.Infrastructure;

public class MultiplatformTheoryAttribute : TheoryAttribute
{
    private readonly RavenPlatform _platform;
    private readonly RavenArchitecture _architecture;

    public MultiplatformTheoryAttribute(RavenPlatform platform = RavenPlatform.All)
        : this(platform, RavenArchitecture.All)
    {
    }

    public MultiplatformTheoryAttribute(RavenArchitecture architecture = RavenArchitecture.All)
        : this(RavenPlatform.All, architecture)
    {
    }

    public MultiplatformTheoryAttribute(RavenPlatform platform = RavenPlatform.All, RavenArchitecture architecture = RavenArchitecture.All)
    {
        _platform = platform;
        _architecture = architecture;
    }

    public override string Skip => MultiplatformFactAttribute.ShouldSkip(_platform, _architecture);
}
