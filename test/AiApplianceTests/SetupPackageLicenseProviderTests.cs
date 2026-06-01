using System;
using System.IO;
using FastTests;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.AiHelper;
using Raven.AiAppliance.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

public class SetupPackageLicenseProviderTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Returns_typed_license_when_license_json_present()
    {
        var dir = NewTempDir();
        File.WriteAllText(Path.Combine(dir, "license.json"), """{"Id":"abc","Name":"test","Keys":["k1","k2"]}""");

        var provider = new SetupPackageLicenseProvider(
            Options.Create(new ApplianceOptions { SetupPackagePath = dir }));

        Assert.True(provider.TryGetLicense(out var license));
        Assert.Equal("abc", license.Id);
        Assert.Equal("test", license.Name);
        Assert.Equal(new[] { "k1", "k2" }, license.Keys);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Returns_false_when_license_json_missing()
    {
        var dir = NewTempDir();

        var provider = new SetupPackageLicenseProvider(
            Options.Create(new ApplianceOptions { SetupPackagePath = dir }));

        Assert.False(provider.TryGetLicense(out _));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Returns_false_when_license_json_has_no_id()
    {
        var dir = NewTempDir();
        File.WriteAllText(Path.Combine(dir, "license.json"), """{"Name":"test"}""");

        var provider = new SetupPackageLicenseProvider(
            Options.Create(new ApplianceOptions { SetupPackagePath = dir }));

        Assert.False(provider.TryGetLicense(out _));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Returns_false_when_license_json_cannot_be_read()
    {
        // Contract: return false when absent or unreadable; callers degrade to InvalidCredentials.
        // Hold an exclusive lock so the read throws IOException; the provider must swallow it,
        // not let it escape as a 500.
        var dir = NewTempDir();
        var path = Path.Combine(dir, "license.json");
        File.WriteAllText(path, """{"Id":"abc","Name":"test","Keys":["k1"]}""");

        using var locked = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None);

        var provider = new SetupPackageLicenseProvider(
            Options.Create(new ApplianceOptions { SetupPackagePath = dir }));

        Assert.False(provider.TryGetLicense(out _));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public void Returns_false_when_license_json_is_malformed()
    {
        var dir = NewTempDir();
        File.WriteAllText(Path.Combine(dir, "license.json"), "{ this is not json");

        var provider = new SetupPackageLicenseProvider(
            Options.Create(new ApplianceOptions { SetupPackagePath = dir }));

        Assert.False(provider.TryGetLicense(out _));
    }

    private static string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), nameof(SetupPackageLicenseProviderTests), Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }
}
