using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public sealed class UserDomainsAndLicenseInfo
    {
        public UserDomainsWithIps UserDomainsWithIps { get; set; }
        public LicenseStatus LicenseStatus { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(UserDomainsWithIps)] = UserDomainsWithIps.ToJson(),
                [nameof(LicenseStatus)] = LicenseStatus?.ToJson()
            };
        }
    }
}
