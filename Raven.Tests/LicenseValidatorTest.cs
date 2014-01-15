using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using Rhino.Licensing;
using Xunit;

namespace Raven.Tests
{
    public class LicenseValidatorTest
    {
        [Fact]
        public void An_expired_OEM_license_is_still_valid()
        {
            var rsa = new RSACryptoServiceProvider();
            var licenseGenerator = new LicenseGenerator(rsa.ToXmlString(true));
            var licenseValues = new Dictionary<string, string>
            {
                {"OEM", "true"},
            };

            var license = licenseGenerator.Generate("Foo", Guid.NewGuid(), new DateTime(2000, 1, 1), licenseValues, LicenseType.Subscription);
            var licenseValidator = new StringLicenseValidator(rsa.ToXmlString(false), license)
			{
				DisableFloatingLicenses = true,
				SubscriptionEndpoint = "http://uberprof.com/Subscriptions.svc"
			};
            licenseValidator.AssertValidLicense();
        }

        [Fact]
        public void An_expired_non_OEM_license_is_invalid()
        {
            var rsa = new RSACryptoServiceProvider();
            var licenseGenerator = new LicenseGenerator(rsa.ToXmlString(true));
            var licenseValues = new Dictionary<string, string>();

            var license = licenseGenerator.Generate("Foo", Guid.NewGuid(), new DateTime(2000, 1, 1), licenseValues, LicenseType.Subscription);
            var licenseValidator = new StringLicenseValidator(rsa.ToXmlString(false), license)
            {
                DisableFloatingLicenses = true,
                SubscriptionEndpoint = "http://uberprof.com/Subscriptions.svc"
            };
            Assert.Throws<LicenseExpiredException>(() => licenseValidator.AssertValidLicense());
        }

        [Fact]
        public void An_not_expired_non_OEM_license_is_valid()
        {
            var rsa = new RSACryptoServiceProvider();
            var licenseGenerator = new LicenseGenerator(rsa.ToXmlString(true));
            var licenseValues = new Dictionary<string, string>();

            var license = licenseGenerator.Generate("Foo", Guid.NewGuid(), DateTime.Today.AddDays(1), licenseValues, LicenseType.Subscription);
            var licenseValidator = new StringLicenseValidator(rsa.ToXmlString(false), license)
            {
                DisableFloatingLicenses = true,
                SubscriptionEndpoint = "http://uberprof.com/Subscriptions.svc"
            };
            licenseValidator.AssertValidLicense();
        }
    }
}
