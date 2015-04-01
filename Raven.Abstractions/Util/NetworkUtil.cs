using System;
using System.Linq;
using System.Net;

namespace Raven.Abstractions
{
	public static class NetworkUtil
	{
		public static bool IsLocalhost(string hostNameOrAddress)
		{
			if (string.IsNullOrEmpty(hostNameOrAddress))
				return false;

			try
			{
				var hostIPs = new IPAddress[0];
				if (Uri.IsWellFormedUriString(hostNameOrAddress,UriKind.RelativeOrAbsolute))
				{
					var uri = new Uri(hostNameOrAddress);
					hostIPs = Dns.GetHostAddresses(uri.DnsSafeHost);
				}

				var localIPs = Dns.GetHostAddresses(Dns.GetHostName());

				return hostIPs.Any(hostIP => IPAddress.IsLoopback(hostIP) || 
											 localIPs.Contains(hostIP));
			}
			catch
			{
				return false;
			}
		}
	}
}
