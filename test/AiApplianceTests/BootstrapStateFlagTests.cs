using System.IO;
using FastTests;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// <see cref="BootstrapStateFlag"/> derives its startup phase — and the <c>StartedWithSetupPackage</c>
/// flag the readiness service gates its bootstrap-Ready flip on — from whether the setup package
/// (<c>A/settings.json</c>) is on disk at construction. This is the discriminator that keeps the
/// first/unsecured start (package appears mid-process) from racing the post-restart secure start.
/// </summary>
public class BootstrapStateFlagTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Without_setup_package_starts_NeedsActivation()
    {
        IBootstrapState state = new BootstrapStateFlag(OptionsFor(NewDataPath(forceCreateDir: true)));

        Assert.False(state.StartedWithSetupPackage);
        Assert.Equal(BootstrapPhase.NeedsActivation, state.Phase);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void With_setup_package_present_starts_Restarting()
    {
        var dir = NewDataPath(forceCreateDir: true);
        var settings = Path.Combine(dir, "A", "settings.json");
        Directory.CreateDirectory(Path.GetDirectoryName(settings)!);
        File.WriteAllText(settings, "{}");

        IBootstrapState state = new BootstrapStateFlag(OptionsFor(dir));

        Assert.True(state.StartedWithSetupPackage);
        Assert.Equal(BootstrapPhase.Restarting, state.Phase);
    }

    private static IOptions<ApplianceOptions> OptionsFor(string setupPackagePath) =>
        Microsoft.Extensions.Options.Options.Create(new ApplianceOptions { SetupPackagePath = setupPackagePath });
}
