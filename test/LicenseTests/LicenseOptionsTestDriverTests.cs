using System;
using System.IO;
using EmbeddedTests.TestDriver;
using FastTests;
using LicenseTests.Helpers;
using Raven.Embedded;
using Raven.TestDriver;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace LicenseTests;

public sealed record LicenseOptionsTestDriverScenario(
    bool? ThrowOnInvalidOrMissingLicense,
    string LicenseToTest,
    LicenseSource LicenseSource,
    string ConfigKeyToTest,
    bool ExpectSuccess,
    Action<Exception, string> Assertion = null);

[Collection("TestCollection.NonParallelTests")]
public class LicenseOptionsTestDriverTests : RavenTestBase
{
    private readonly string _serverDirectory = TestDriverExampleTest.GetServerPath();

    public LicenseOptionsTestDriverTests(ITestOutputHelper output) : base(output)
    {
    }

    private void RunScenario(LicenseOptionsTestDriverScenario optionsTestDriverScenario)
    {
        var originalLicense = Environment.GetEnvironmentVariable("RAVEN_License");
        var originalLicensePath = Environment.GetEnvironmentVariable("RAVEN_License.Path");

        var testServerOptions = new TestServerOptions { ServerDirectory = _serverDirectory };

        if (optionsTestDriverScenario.ThrowOnInvalidOrMissingLicense.HasValue)
            testServerOptions.Licensing = new ServerOptions.LicensingOptions { ThrowOnInvalidOrMissingLicense = optionsTestDriverScenario.ThrowOnInvalidOrMissingLicense.Value };

        using (var driver = new RavenTestDriver())
        using (driver.ConfigureScopedServer(testServerOptions))
        {
            try
            {
                LicenseOptionTestHelper.ProcessLicenseOptions(optionsTestDriverScenario.LicenseToTest, optionsTestDriverScenario.LicenseSource, optionsTestDriverScenario.ConfigKeyToTest, testServerOptions);

                if (optionsTestDriverScenario.ExpectSuccess)
                {
                    using var _ = driver.GetDocumentStore();
                }
                else
                {
                    var exception = Assert.Throws<InvalidOperationException>(() => driver.GetDocumentStore());
                    optionsTestDriverScenario.Assertion?.Invoke(exception, _serverDirectory);
                }
            }
            finally
            {
                Environment.SetEnvironmentVariable("RAVEN_License", originalLicense);
                Environment.SetEnvironmentVariable("RAVEN_License.Path", originalLicensePath);

                var path = Path.Combine(_serverDirectory, "license.json");
                if (File.Exists(path)) File.Delete(path);
            }
        }
    }

    #region Success-expected scenarios

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_EnvVar_License_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: false,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_ServerOption_License_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: false,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_EnvVar_LicensePath_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: false,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_ServerOption_LicensePath_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: false,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_EnvVar_License_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: LicenseOptionTestHelper.ValidLicense,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_ServerOption_License_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: LicenseOptionTestHelper.ValidLicense,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_EnvVar_LicensePath_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: LicenseOptionTestHelper.ValidLicense,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_ServerOption_LicensePath_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: LicenseOptionTestHelper.ValidLicense,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_EnvVar_License_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: null,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_ServerOption_License_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: null,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_EnvVar_LicensePath_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: null,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_ServerOption_LicensePath_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: null,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_NoLicense_EnvVar_License_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: null,
            LicenseToTest: null,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_NoLicense_ServerOption_License_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: null,
            LicenseToTest: null,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_NoLicense_EnvVar_LicensePath_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: null,
            LicenseToTest: null,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_NoLicense_ServerOption_LicensePath_ShouldWork()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: null,
            LicenseToTest: null,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: true);

        RunScenario(scenario);
    }

    #endregion

    #region Failure-expected scenarios

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_EnvVar_License_ShouldThrow()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: false,
            Assertion: FailureAssertions.Assert_InvalidLicense_LicenseConfig());

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_EnvVar_LicensePath_ShouldThrow()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: false,
            Assertion: FailureAssertions.Assert_InvalidLicense_LicensePathConfig());

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_ServerOption_License_ShouldThrow()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: false,
            Assertion: FailureAssertions.Assert_InvalidLicense_LicenseConfig());

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_ServerOption_LicensePath_ShouldThrow()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: LicenseOptionTestHelper.InvalidLicense,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: false,
            Assertion: FailureAssertions.Assert_InvalidLicense_LicensePathConfig());

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_EnvVar_License_ShouldThrow()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: null,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: false,
            Assertion: FailureAssertions.Assert_NoLicense());

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_ServerOption_License_ShouldThrow()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: null,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicenseConfigurationKey,
            ExpectSuccess: false,
            Assertion: FailureAssertions.Assert_NoLicense());

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_EnvVar_LicensePath_ShouldThrow()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: null,
            LicenseSource: LicenseSource.EnvironmentVariable,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: false,
            Assertion: FailureAssertions.Assert_NoLicense());

        RunScenario(scenario);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_ServerOption_LicensePath_ShouldThrow()
    {
        var scenario = new LicenseOptionsTestDriverScenario(
            ThrowOnInvalidOrMissingLicense: true,
            LicenseToTest: null,
            LicenseSource: LicenseSource.ServerOption,
            ConfigKeyToTest: LicenseOptionTestHelper.LicensePathConfigurationKey,
            ExpectSuccess: false,
            Assertion: FailureAssertions.Assert_NoLicense());

        RunScenario(scenario);
    }

    #endregion
}
