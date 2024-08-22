using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using EmbeddedTests;
using FastTests;
using Raven.Client.Exceptions.Server;
using Raven.Embedded;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace LicenseTests;

[Collection("TestCollection.NonParallelTests")]
public class LicenseOptionsEmbeddedTests : EmbeddedTestBase
{
    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_SystemEnvironmentVariableLicence_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: false, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_ServerOptionLicence_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: false, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_SystemEnvironmentVariableLicencePath_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: false, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_ServerOptionLicencePath_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: false, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_SystemEnvironmentVariableLicence_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.ValidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_ServerOptionLicence_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.ValidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_SystemEnvironmentVariableLicencePath_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.ValidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_ServerOptionLicencePath_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.ValidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_SystemEnvironmentVariableLicense_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
                StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_ServerOptionLicense_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_SystemEnvironmentVariableLicensePath_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_ServerOptionLicensePath_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_SystemEnvironmentVariableLicense_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
                StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_ServerOptionLicense_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_SystemEnvironmentVariableLicensePath_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_ServerOptionLicensePath_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_SystemEnvironmentVariableLicense_ShouldThrow()
    {
        ServerOptions options = new();
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTestWithDefaultThrowOnInvalidOrMissingLicenseValue(LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_ServerOptionLicense_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTestWithDefaultThrowOnInvalidOrMissingLicenseValue(LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_SystemEnvironmentVariableLicensePath_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTestWithDefaultThrowOnInvalidOrMissingLicenseValue(LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_ServerOptionLicensePath_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTestWithDefaultThrowOnInvalidOrMissingLicenseValue(LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    private void StartEmbeddedServerLicenseOptionTest(bool throwOnInvalidOrMissingLicense, string license, LicenseSource licenseSource, string configurationKeyToTest, out ServerOptions options)
    {
        var originalLicense = Environment.GetEnvironmentVariable("RAVEN_License");
        var originalLicensePath = Environment.GetEnvironmentVariable("RAVEN_License.Path");

        options = CopyServerAndCreateOptions();
        options.LicenseConfiguration.ThrowOnInvalidOrMissingLicense = throwOnInvalidOrMissingLicense;

        try
        {
            if (configurationKeyToTest == LicenseOptionTestHelper.LicenseConfigurationKey)
            {
                HandleLicenseOption(license, licenseSource, ref options);
            }
            else if (configurationKeyToTest == LicenseOptionTestHelper.LicensePathConfigurationKey)
            {
                HandleLicensePathOption(license, licenseSource, ref options);
            }

            CreateEmbeddedServer(options);
        }
        catch
        {
            Task.Delay(1000).Wait(); // wait to ensure the server is fully disposed
            throw;
        }
        finally
        {
            AfterTestCleanup(originalLicense, originalLicensePath);
        }
    }

    private void StartEmbeddedServerLicenseOptionTestWithDefaultThrowOnInvalidOrMissingLicenseValue(string license, LicenseSource licenseSource, string configurationKeyToTest, out ServerOptions options)
    {
        var originalLicense = Environment.GetEnvironmentVariable("RAVEN_License");
        var originalLicensePath = Environment.GetEnvironmentVariable("RAVEN_License.Path");

        options = CopyServerAndCreateOptions();

        try
        {
            if (configurationKeyToTest == LicenseOptionTestHelper.LicenseConfigurationKey)
            {
                HandleLicenseOption(license, licenseSource, ref options);
            }
            else if (configurationKeyToTest == LicenseOptionTestHelper.LicensePathConfigurationKey)
            {
                HandleLicensePathOption(license, licenseSource, ref options);
            }

            CreateEmbeddedServer(options);
        }
        catch
        {
            Task.Delay(1000).Wait(); // wait to ensure the server is fully disposed
            throw;
        }
        finally
        {
            AfterTestCleanup(originalLicense, originalLicensePath);
        }
    }

    private static void HandleLicenseOption(string license, LicenseSource licenseSource, ref ServerOptions options)
    {
        switch (licenseSource)
        {
            case LicenseSource.EnvironmentVariable:
                Assert.Null(options.LicenseConfiguration.License);
                Assert.Null(options.LicenseConfiguration.LicensePath);

                Environment.SetEnvironmentVariable("RAVEN_License", license);
                Environment.SetEnvironmentVariable("RAVEN_License.Path", null);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License"), license);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License.Path"), null);
                break;

            case LicenseSource.ServerOption:
                options.LicenseConfiguration.License = license;
                Assert.Null(options.LicenseConfiguration.LicensePath);

                Environment.SetEnvironmentVariable("RAVEN_License", null);
                Environment.SetEnvironmentVariable("RAVEN_License.Path", null);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License"), null);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License.Path"), null);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(licenseSource), licenseSource, null);
        }
    }

    private static void HandleLicensePathOption(string license, LicenseSource licenseSource, ref ServerOptions options)
    {
        string licensePath = LicenseOptionTestHelper.CreateLicenseJsonFile(options.ServerDirectory, license);

        switch (licenseSource)
        {
            case LicenseSource.EnvironmentVariable:
                Assert.Null(options.LicenseConfiguration.License);
                Assert.Null(options.LicenseConfiguration.LicensePath);

                Environment.SetEnvironmentVariable("RAVEN_License", null);
                Environment.SetEnvironmentVariable("RAVEN_License.Path", licensePath);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License"), null);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License.Path"), licensePath);
                break;

            case LicenseSource.ServerOption:
                options.LicenseConfiguration.LicensePath = licensePath;
                Assert.Null(options.LicenseConfiguration.License);

                Environment.SetEnvironmentVariable("RAVEN_License", null);
                Environment.SetEnvironmentVariable("RAVEN_License.Path", null);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License"), null);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License.Path"), null);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(licenseSource), licenseSource, null);
        }
    }

    private static void CreateEmbeddedServer(ServerOptions options)
    {
        RavenServerRunner.ForTestingPurposesOnly().EnvironmentVariablesToCopyToInternalProcess = new List<string> { "RAVEN_License", "RAVEN_License.Path" };

        using (var embedded = new EmbeddedServer())
        {
            embedded.StartServer(options);
        }
    }

    private static void AfterTestCleanup(string originalLicense, string originalLicensePath)
    {
        RavenServerRunner.ForTestingPurposesOnly().EnvironmentVariablesToCopyToInternalProcess = null;

        Environment.SetEnvironmentVariable("RAVEN_License", originalLicense);
        Environment.SetEnvironmentVariable("RAVEN_License.Path", originalLicensePath);
    }
}

[Collection("TestCollection.NonParallelTests")]
public class LicenseOptionsTestDriverTests : RavenTestBase
{
    public LicenseOptionsTestDriverTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_SystemEnvironmentVariableLicence_ShouldWork()
    {
        StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: false, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_ServerOptionLicence_ShouldWork()
    {
        StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: false, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_SystemEnvironmentVariableLicencePath_ShouldWork()
    {
        StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: false, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceFalse_InvalidLicense_ServerOptionLicencePath_ShouldWork()
    {
        StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: false, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_SystemEnvironmentVariableLicence_ShouldWork()
    {
        StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.ValidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_ServerOptionLicence_ShouldWork()
    {
        StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.ValidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_SystemEnvironmentVariableLicencePath_ShouldWork()
    {
        StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.ValidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_ValidLicense_ServerOptionLicencePath_ShouldWork()
    {
        StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.ValidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_SystemEnvironmentVariableLicense_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(Environment.CurrentDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_ServerOptionLicense_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(Environment.CurrentDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_SystemEnvironmentVariableLicensePath_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_ServerOptionLicensePath_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_SystemEnvironmentVariableLicense_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(Environment.CurrentDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_ServerOptionLicense_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(Environment.CurrentDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_SystemEnvironmentVariableLicensePath_ShouldThrow()
    {
        ServerCreationOptions options = null;
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        Assert.True(options.CustomSettings.TryGetValue("ForTestingPurposesLicensePath", out var licensePath));
        var readErrorException = new FileNotFoundException($"Could not find file '{licensePath}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_ServerOptionLicensePath_ShouldThrow()
    {
        ServerCreationOptions options = null;
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        Assert.True(options.CustomSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Licensing.LicensePath), out var licensePath));
        var readErrorException = new FileNotFoundException($"Could not find file '{licensePath}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_SystemEnvironmentVariableLicense_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTestWithDefaultThrowOnInvalidOrMissingLicenseValue(LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(Environment.CurrentDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_ServerOptionLicense_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTestWithDefaultThrowOnInvalidOrMissingLicenseValue(LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(Environment.CurrentDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_SystemEnvironmentVariableLicensePath_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTestWithDefaultThrowOnInvalidOrMissingLicenseValue(LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_ServerOptionLicensePath_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(ServerLoadFailureException), () =>
            StartTestDriverServerLicenseOptionTestWithDefaultThrowOnInvalidOrMissingLicenseValue(LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseHelper.LicenseVerificationErrorBuilder();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertException<LicenseExpiredException>(exception, expectedMessageBuilder);
    }

    private void StartTestDriverServerLicenseOptionTest(bool throwOnInvalidOrMissingLicense, string license, LicenseSource licenseSource, string configurationKeyToTest, out ServerCreationOptions options)
    {
        var originalLicense = Environment.GetEnvironmentVariable("RAVEN_License");
        var originalLicensePath = Environment.GetEnvironmentVariable("RAVEN_License.Path");

        options = new ServerCreationOptions { CustomSettings = new Dictionary<string, string>() };
        options.CustomSettings[RavenConfiguration.GetKey(x => x.Licensing.ThrowOnInvalidOrMissingLicense)] = throwOnInvalidOrMissingLicense.ToString();

        try
        {
            if (configurationKeyToTest == LicenseOptionTestHelper.LicenseConfigurationKey)
            {
                HandleLicenseOption(license, licenseSource, ref options);
            }
            else if (configurationKeyToTest == LicenseOptionTestHelper.LicensePathConfigurationKey)
            {
                HandleLicensePathOption(license, licenseSource, ref options);
            }

            using (_ = GetNewServer(options))
            {
            }
        }
        catch
        {
            Task.Delay(1000).Wait(); // wait to ensure the server is fully disposed
            throw;
        }
        finally
        {
            AfterTestCleanup(originalLicense, originalLicensePath);
        }
    }

    private void StartTestDriverServerLicenseOptionTestWithDefaultThrowOnInvalidOrMissingLicenseValue(string license, LicenseSource licenseSource, string configurationKeyToTest, out ServerCreationOptions options)
    {
        var originalLicense = Environment.GetEnvironmentVariable("RAVEN_License");
        var originalLicensePath = Environment.GetEnvironmentVariable("RAVEN_License.Path");

        options = new ServerCreationOptions { CustomSettings = new Dictionary<string, string>() };

        try
        {
            if (configurationKeyToTest == LicenseOptionTestHelper.LicenseConfigurationKey)
            {
                HandleLicenseOption(license, licenseSource, ref options);
            }
            else if (configurationKeyToTest == LicenseOptionTestHelper.LicensePathConfigurationKey)
            {
                HandleLicensePathOption(license, licenseSource, ref options);
            }

            using (_ = GetNewServer(options))
            {
            }
        }
        catch
        {
            Task.Delay(1000).Wait(); // wait to ensure the server is fully disposed
            throw;
        }
        finally
        {
            AfterTestCleanup(originalLicense, originalLicensePath);
        }
    }

    internal static void HandleLicenseOption(string license, LicenseSource licenseSource, ref ServerCreationOptions options)
    {
        switch (licenseSource)
        {
            case LicenseSource.EnvironmentVariable:
                Assert.False(options.CustomSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Licensing.License), out _));
                Assert.False(options.CustomSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Licensing.LicensePath), out _));

                Environment.SetEnvironmentVariable("RAVEN_License", license);
                Environment.SetEnvironmentVariable("RAVEN_License.Path", null);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License"), license);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License.Path"), null);
                break;

            case LicenseSource.ServerOption:
                options.CustomSettings[RavenConfiguration.GetKey(x => x.Licensing.License)] = license;
                Assert.False(options.CustomSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Licensing.LicensePath), out _));

                Environment.SetEnvironmentVariable("RAVEN_License", null);
                Environment.SetEnvironmentVariable("RAVEN_License.Path", null);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License"), null);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License.Path"), null);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(licenseSource), licenseSource, null);
        }
    }

    private void HandleLicensePathOption(string license, LicenseSource licenseSource, ref ServerCreationOptions options)
    {
        var dataPath = NewDataPath(forceCreateDir: true);
        string licensePath = LicenseOptionTestHelper.CreateLicenseJsonFile(dataPath, license);

        switch (licenseSource)
        {
            case LicenseSource.EnvironmentVariable:
                options.CustomSettings["ForTestingPurposesLicensePath"] = licensePath;
                Assert.False(options.CustomSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Licensing.License), out _));
                Assert.False(options.CustomSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Licensing.LicensePath), out _));

                Environment.SetEnvironmentVariable("RAVEN_License", null);
                Environment.SetEnvironmentVariable("RAVEN_License.Path", licensePath);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License"), null);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License.Path"), licensePath);
                break;

            case LicenseSource.ServerOption:
                options.CustomSettings[RavenConfiguration.GetKey(x => x.Licensing.LicensePath)] = licensePath;
                Assert.False(options.CustomSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Licensing.License), out _));

                Environment.SetEnvironmentVariable("RAVEN_License", null);
                Environment.SetEnvironmentVariable("RAVEN_License.Path", null);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License"), null);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_License.Path"), null);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(licenseSource), licenseSource, null);
        }
    }

    private static void AfterTestCleanup(string originalLicense, string originalLicensePath)
    {
        Environment.SetEnvironmentVariable("RAVEN_License", originalLicense);
        Environment.SetEnvironmentVariable("RAVEN_License.Path", originalLicensePath);
    }
}

internal enum LicenseSource
{
    EnvironmentVariable,
    ServerOption
}

[CollectionDefinition("TestCollection.NonParallelTests", DisableParallelization = true)]
public class NonParallelRavenTestsCollection
{
    // just a definition to group tests to run in non-parallel mode
}

public static class LicenseOptionTestHelper
{
    internal const string InvalidLicense = "SomeInvalidLicense";
    internal static readonly string LicenseConfigurationKey = RavenConfiguration.GetKey(x => x.Licensing.License);
    internal static readonly string LicensePathConfigurationKey = RavenConfiguration.GetKey(x => x.Licensing.LicensePath);
    internal static readonly string ValidLicense = Environment.GetEnvironmentVariable("RAVEN_LICENSE");

    public static string CreateLicenseJsonFile(string directoryPath, string licenseToTest)
    {
        var licenseJsonPath = Path.Combine(directoryPath, "license.json");
        if (File.Exists(licenseJsonPath))
            File.Delete(licenseJsonPath);

        if (string.IsNullOrWhiteSpace(licenseToTest))
            return licenseJsonPath;

        File.WriteAllText(licenseJsonPath, licenseToTest);

        Assert.True(File.Exists(licenseJsonPath));
        Assert.Equal(File.ReadAllText(licenseJsonPath), licenseToTest);

        return licenseJsonPath;
    }

    internal static void AssertException<T>(Exception exception, LicenseHelper.LicenseVerificationErrorBuilder expectedMessageBuilder) where T:Exception
    {
        Assert.NotNull(exception.InnerException);
        Assert.IsType<T>(exception.InnerException);
        Assert.True(exception.InnerException.Message.Contains(expectedMessageBuilder.ToString()),
            userMessage: $"Exception message: {exception.InnerException.Message}{Environment.NewLine}But expected message:{Environment.NewLine}{expectedMessageBuilder}");
    }
}
