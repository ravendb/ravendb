using System;
using System.IO;

namespace LicenseTests.Helpers;

internal static class FailureAssertions
{
    private static FileNotFoundException GenerateLicenseFileNotFoundException(string serverDirectory) => new($"Could not find file '{Path.Combine(serverDirectory, "license.json")}'.");

    internal static Action<Exception, string> Assert_InvalidLicense_LicenseConfig() =>
        (exception, serverDirectory) =>
        {
            var builder = new LicenseVerificationErrorBuilderForTestingPurposes();
            builder.AppendLicenseMissingMessage();
            builder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
            builder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);
            builder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
            builder.AppendFileReadErrorMessage(GenerateLicenseFileNotFoundException(serverDirectory));
            builder.AppendGeneralSuggestions();
            builder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

            LicenseOptionTestHelper.AssertLicenseVerificationException(exception, builder);
        };

    internal static Action<Exception, string> Assert_InvalidLicense_LicensePathConfig() =>
        (exception, _) =>
        {
            var builder = new LicenseVerificationErrorBuilderForTestingPurposes();
            builder.AppendLicenseMissingMessage();
            builder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
            builder.AppendDeserializationErrorMessage(licenseContent: null);
            builder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
            builder.AppendDeserializationErrorMessage(LicenseOptionTestHelper.InvalidLicense);
            builder.AppendGeneralSuggestions();
            builder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

            LicenseOptionTestHelper.AssertLicenseVerificationException(exception, builder);
        };

    internal static Action<Exception, string> Assert_NoLicense() =>
        (exception, serverDirectory) =>
        {
            var builder = new LicenseVerificationErrorBuilderForTestingPurposes();
            builder.AppendLicenseMissingMessage();
            builder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicenseConfigurationKey);
            builder.AppendDeserializationErrorMessage(licenseContent: null);
            builder.AppendConfigurationKeyUsageAttempt(LicenseOptionTestHelper.LicensePathConfigurationKey);
            builder.AppendFileReadErrorMessage(GenerateLicenseFileNotFoundException(serverDirectory));
            builder.AppendGeneralSuggestions();
            builder.AppendSuggestionToDisableThrowOnInvalidOrMissingLicenseOption(throwOnInvalidOrMissingLicenseOptionEnabled: true, isInStorageLicenseExpired: false);

            LicenseOptionTestHelper.AssertLicenseVerificationException(exception, builder);
        };
}
