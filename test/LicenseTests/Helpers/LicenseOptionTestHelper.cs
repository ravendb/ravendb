using System;
using System.IO;
using Raven.Embedded;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Xunit;

namespace LicenseTests.Helpers;

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


    static string Normalize(string s) => s.Replace("\r\n", "\n");

    internal static void AssertLicenseVerificationException(Exception exception, LicenseHelper.LicenseVerificationErrorBuilder expectedMessageBuilder)
    {
        Assert.True(Normalize(exception.Message).Contains(Normalize(expectedMessageBuilder.ToString())),
            userMessage: $"Exception message: {exception.Message}{Environment.NewLine}But expected message:{Environment.NewLine}{expectedMessageBuilder}");
    }

    internal static void AssertInnerLicenseVerificationException<T>(Exception exception, LicenseHelper.LicenseVerificationErrorBuilder expectedMessageBuilder) where T:Exception
    {
        Assert.NotNull(exception.InnerException);
        Assert.IsType<T>(exception.InnerException);
        Assert.True(Normalize(exception.InnerException.Message).Contains(Normalize(expectedMessageBuilder.ToString())),
            userMessage: $"Exception message: {exception.InnerException.Message}{Environment.NewLine}But expected message:{Environment.NewLine}{expectedMessageBuilder}");
    }

    internal static void ProcessLicenseOptions(string license, LicenseSource licenseSource, string configurationKeyToTest, ServerOptions options)
    {
        if (configurationKeyToTest == LicenseConfigurationKey)
            HandleLicenseOption(license, licenseSource, options);

        else if (configurationKeyToTest == LicensePathConfigurationKey)
            HandleLicensePathOption(license, licenseSource, options);
    }

    private static void HandleLicenseOption(string license, LicenseSource licenseSource, ServerOptions options)
    {
        switch (licenseSource)
        {
            case LicenseSource.EnvironmentVariable:
                Assert.Null(options.Licensing.License);
                Assert.Null(options.Licensing.LicensePath);

                Environment.SetEnvironmentVariable("RAVEN_LICENSE", license);
                Environment.SetEnvironmentVariable("RAVEN_LICENSE_PATH", null);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_LICENSE"), license);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_LICENSE_PATH"), null);
                break;

            case LicenseSource.ServerOption:
                options.Licensing.License = license;
                Assert.Null(options.Licensing.LicensePath);

                Environment.SetEnvironmentVariable("RAVEN_LICENSE", null);
                Environment.SetEnvironmentVariable("RAVEN_LICENSE_PATH", null);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_LICENSE"), null);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_LICENSE_PATH"), null);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(licenseSource), licenseSource, null);
        }
    }

    private static void HandleLicensePathOption(string license, LicenseSource licenseSource, ServerOptions options)
    {
        string licensePath = CreateLicenseJsonFile(options.ServerDirectory, license);

        switch (licenseSource)
        {
            case LicenseSource.EnvironmentVariable:
                Assert.Null(options.Licensing.License);
                Assert.Null(options.Licensing.LicensePath);

                Environment.SetEnvironmentVariable("RAVEN_LICENSE", null);
                Environment.SetEnvironmentVariable("RAVEN_LICENSE_PATH", licensePath);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_LICENSE"), null);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_LICENSE_PATH"), licensePath);
                break;

            case LicenseSource.ServerOption:
                options.Licensing.LicensePath = licensePath;
                Assert.Null(options.Licensing.License);

                Environment.SetEnvironmentVariable("RAVEN_LICENSE", null);
                Environment.SetEnvironmentVariable("RAVEN_LICENSE_PATH", null);

                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_LICENSE"), null);
                Assert.Equal(Environment.GetEnvironmentVariable("RAVEN_LICENSE_PATH"), null);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(licenseSource), licenseSource, null);
        }
    }
}
