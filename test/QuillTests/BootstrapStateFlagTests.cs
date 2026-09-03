using FastTests;
using Microsoft.Extensions.Options;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class BootstrapStateFlagTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Without_setup_package_starts_NeedsActivation()
    {
        IBootstrapState state = new BootstrapStateFlag(OptionsFor(NewDataPath(forceCreateDir: true)));

        Assert.False(state.StartedWithSetupPackage);
        Assert.Equal(BootstrapPhase.NeedsActivation, state.Phase);
    }

    [RavenFact(RavenTestCategory.Quill)]
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
