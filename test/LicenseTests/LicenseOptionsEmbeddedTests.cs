using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using EmbeddedTests;
using LicenseTests.Helpers;
using Raven.Embedded;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

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
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_EnvVar_License_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: null, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_ServerOption_License_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: null, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_EnvVar_LicensePath_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: null, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_InvalidLicense_ServerOption_LicensePath_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: null, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_NoLicense_EnvVar_License_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: null, license: null, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_NoLicense_ServerOption_License_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: null, license: null, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_NoLicense_EnvVar_LicensePath_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: null, license: null, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceDefaultValue_NoLicense_ServerOption_LicensePath_ShouldWork()
    {
        StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: null, license: null, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out _);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_SystemEnvironmentVariableLicense_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
                StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseVerificationErrorBuilderForTestingPurposes();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertInnerLicenseVerificationException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_ServerOptionLicense_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseVerificationErrorBuilderForTestingPurposes();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertInnerLicenseVerificationException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_SystemEnvironmentVariableLicensePath_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseVerificationErrorBuilderForTestingPurposes();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertInnerLicenseVerificationException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_InvalidLicense_ServerOptionLicensePath_ShouldThrow()
    {
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, LicenseOptionTestHelper.InvalidLicense, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out _));

        var expectedMessageBuilder = new LicenseVerificationErrorBuilderForTestingPurposes();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertInnerLicenseVerificationException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_SystemEnvironmentVariableLicense_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
                StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicenseConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseVerificationErrorBuilderForTestingPurposes();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertInnerLicenseVerificationException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_ServerOptionLicense_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.ServerOption, LicenseOptionTestHelper.LicenseConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseVerificationErrorBuilderForTestingPurposes();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertInnerLicenseVerificationException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_SystemEnvironmentVariableLicensePath_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.EnvironmentVariable, LicenseOptionTestHelper.LicensePathConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseVerificationErrorBuilderForTestingPurposes();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertInnerLicenseVerificationException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    [RavenFact(RavenTestCategory.Embedded | RavenTestCategory.Licensing)]
    public void VerifyLicense_EnforceTrue_NoLicense_ServerOptionLicensePath_ShouldThrow()
    {
        ServerOptions options = null;
        var exception = Assert.Throws(typeof(AggregateException), () =>
            StartEmbeddedServerLicenseOptionTest(throwOnInvalidOrMissingLicense: true, license: null, LicenseSource.ServerOption, LicenseOptionTestHelper.LicensePathConfigurationKey, out options));

        var expectedMessageBuilder = new LicenseVerificationErrorBuilderForTestingPurposes();
        expectedMessageBuilder.AppendLicenseMissingMessage();

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
        expectedMessageBuilder.AppendDeserializationErrorMessage(licenseContent: null);

        expectedMessageBuilder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
        var readErrorException = new FileNotFoundException($"Could not find file '{Path.Combine(options.ServerDirectory, "license.json")}'.");
        expectedMessageBuilder.AppendFileReadErrorMessage(readErrorException);

        expectedMessageBuilder.AppendGeneralSuggestions();
        expectedMessageBuilder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

        LicenseOptionTestHelper.AssertInnerLicenseVerificationException<InvalidOperationException>(exception, expectedMessageBuilder);
    }

    private void StartEmbeddedServerLicenseOptionTest(bool? throwOnInvalidOrMissingLicense, string license, LicenseSource licenseSource, string configurationKeyToTest, out ServerOptions options)
    {
        var originalLicense = Environment.GetEnvironmentVariable("RAVEN_License");
        var originalLicensePath = Environment.GetEnvironmentVariable("RAVEN_License.Path");

        options = CopyServerAndCreateOptions();

        if(throwOnInvalidOrMissingLicense.HasValue)
            options.Licensing.ThrowOnInvalidOrMissingLicense = throwOnInvalidOrMissingLicense.Value;

        try
        {
            LicenseOptionTestHelper.ProcessLicenseOptions(license, licenseSource, configurationKeyToTest, options);
            CreateEmbeddedServer(options);
        }
        catch
        {
            Task.Delay(1000).Wait(); // wait to ensure the server is fully disposed
            throw;
        }
        finally
        {
            AfterTestCleanup(originalLicense, originalLicensePath, options);
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

    private static void AfterTestCleanup(string originalLicense, string originalLicensePath, ServerOptions options)
    {
        RavenServerRunner.ForTestingPurposesOnly().EnvironmentVariablesToCopyToInternalProcess = null;

        Environment.SetEnvironmentVariable("RAVEN_License", originalLicense);
        Environment.SetEnvironmentVariable("RAVEN_License.Path", originalLicensePath);

        var path = Path.Combine(options.ServerDirectory, "license.json");
        if (File.Exists(path)) File.Delete(path);
    }
}

public enum LicenseSource
{
    EnvironmentVariable,
    ServerOption
}

[CollectionDefinition("TestCollection.NonParallelTests", DisableParallelization = true)]
public class NonParallelRavenTestsCollection
{
    // just a definition to group tests to run in non-parallel mode
}

public class LicenseVerificationErrorBuilderForTestingPurposes : LicenseHelper.LicenseVerificationErrorBuilder
{
    public LicenseVerificationErrorBuilderForTestingPurposes() : base(null, null, null)
    {
        Configuration = RavenConfiguration.Default;
        Configuration.Embedded.ParentProcessId = 1;
    }
}
