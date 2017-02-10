using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Raven.NewClient.Abstractions
{
    public static class NetworkUtil
    {
        public static async Task<bool> IsLocalhost(string hostNameOrAddress)
        {
            if (string.IsNullOrEmpty(hostNameOrAddress))
                return false;

            try
            {
                var hostIPs = new IPAddress[0];
                if (Uri.IsWellFormedUriString(hostNameOrAddress,UriKind.RelativeOrAbsolute))
                {
                    var uri = new Uri(hostNameOrAddress);
                    hostIPs = await Dns.GetHostAddressesAsync(uri.DnsSafeHost);
                }

                var localIPs = await Dns.GetHostAddressesAsync(Dns.GetHostName());

                return hostIPs.Any(ip => IPAddress.IsLoopback(ip) || localIPs.Contains(ip));
            }
            catch
            {
                return false;
            }
        }
    }
}
