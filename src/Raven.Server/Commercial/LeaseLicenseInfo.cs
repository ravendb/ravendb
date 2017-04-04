using Raven.Client.Server.Operations;

namespace Raven.Server.Commercial
{
    public class LeaseLicenseInfo
    {
        public License License { get; set; }

        public BuildNumber BuildInfo { get; set; }
    }
}