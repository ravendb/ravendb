using System;
using System.Net;
using System.Net.Sockets;
using Raven.Server.Config.Categories;

namespace Raven.Server.Utils
{
    public static class SecurityUtils
    {
        public static bool IsUnsecuredAccessAllowedForAddress(UnsecuredAccessAddressRange addressRange, IPAddress ipAddress)
        {
            switch (ipAddress.AddressFamily)
            {
                case AddressFamily.InterNetwork:
                    return IsUnsecuredAccessAllowedForIPv4Address(addressRange, ipAddress);
                case AddressFamily.InterNetworkV6:
                    return IsUnsecuredAccessAllowedForIPv6Address(addressRange, ipAddress);
                default:
                    throw new NotSupportedException($"Not supported address family: { ipAddress.AddressFamily }");
            }
        }

        private static bool IsUnsecuredAccessAllowedForIPv4Address(
            UnsecuredAccessAddressRange addressRange, IPAddress address)
        {
            var octets = address.GetAddressBytes();

            return (AddressRangeIncludes(addressRange, UnsecuredAccessAddressRange.Local) && octets[0] == 127)
                || (AddressRangeIncludes(addressRange, UnsecuredAccessAddressRange.PrivateNetwork) && 
                        (octets[0] == 10 
                        || (octets[0] == 192 && octets[1] == 168)
                        || (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31)))
                || AddressRangeIncludes(addressRange, UnsecuredAccessAddressRange.PublicNetwork);
        }

        private static bool IsUnsecuredAccessAllowedForIPv6Address(UnsecuredAccessAddressRange addressRange, IPAddress address)
        {
            var octets = address.GetAddressBytes();
            return (AddressRangeIncludes(addressRange, UnsecuredAccessAddressRange.Local) && IsLoopbackIpv6(octets))
                || (AddressRangeIncludes(addressRange, UnsecuredAccessAddressRange.PrivateNetwork) && IsPrivateIpv6(octets))
                || AddressRangeIncludes(addressRange, UnsecuredAccessAddressRange.PublicNetwork);
        }

        private static bool IsLoopbackIpv6(byte[] arr)
        {
            return arr[15] == 0x01;
        }

        private static bool IsPrivateIpv6(byte[] arr)
        {
            return arr[0] == 0xFC // fc00::/7 - https://en.wikipedia.org/wiki/Unique_local_address (similar to IPv4 10.0.0.0/8, 172.16.0.0/12 and 192.168.0.0/16)
                   || ((arr[10] == 0xFF && arr[11] == 0xFF) && 
                        ((arr[12] == 192 && arr[13] == 168)
                            || (arr[12] == 172 && arr[13] >= 16 && arr[13] <= 31)
                            || arr[12] == 10)); // ::ffff:0:0/96 - mapped ipv4 private network addresses (Stateless IP/ICMP Translation (SIIT)) 
        }

        private static bool AddressRangeIncludes(
            UnsecuredAccessAddressRange subjectAddressRange, UnsecuredAccessAddressRange addressRangeToCheck)
        {
            return (subjectAddressRange & addressRangeToCheck) == addressRangeToCheck;
        }
    }
}
