using System;
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

    public void Assert(Feature feature, Func<string> getExceptionMessage = null)
    {
        switch (feature)
        {
            case Feature.GraphApi:
            case Feature.PostgreSql:
                AssertExperimental(feature, getExceptionMessage);
                break;
        }
    }

    public bool CanUse(Feature feature)
    {
        switch (feature)
        {
            case Feature.GraphApi:
            case Feature.PostgreSql:
                return _configuration.Core.FeaturesAvailability == FeaturesAvailability.Experimental;
        }

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AssertExperimental(Feature feature, Func<string> getExceptionMessage = null)
    {
        if (_configuration.Core.FeaturesAvailability == FeaturesAvailability.Stable)
        {
            if (getExceptionMessage == null)
                FeaturesAvailabilityException.Throw(feature);

            FeaturesAvailabilityException.Throw(getExceptionMessage());
        }

        AnyExperimental = true;
    }
}
