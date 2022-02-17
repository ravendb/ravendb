using System.Runtime.CompilerServices;
using Raven.Server.Config;
using Raven.Server.Exceptions;

namespace Raven.Server.Utils.Features;

public class FeatureGuardian
{
    private readonly RavenConfiguration _configuration;

    public bool AnyExperimental;

    public FeatureGuardian(RavenConfiguration configuration)
    {
        _configuration = configuration;
    }

    public void Assert(Feature feature)
    {
        switch (feature)
        {
            case Feature.GraphApi:
                AssertExperimental(feature);
                break;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AssertExperimental(Feature feature)
    {
        if (_configuration.Core.FeaturesAvailability == FeaturesAvailability.Stable)
            FeaturesAvailabilityException.Throw(feature);

        AnyExperimental = true;
    }
}
