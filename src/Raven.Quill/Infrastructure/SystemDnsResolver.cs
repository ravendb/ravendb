using System.Net;
using System.Net.Sockets;

namespace Raven.Quill.Infrastructure;

public sealed class SystemDnsResolver : IDnsResolver
{
    public async Task<string[]> ResolveIPv4Async(string hostname, CancellationToken token)
    {
        try
        {
            var addresses = await Dns.GetHostAddressesAsync(hostname, AddressFamily.InterNetwork, token);
            return Array.ConvertAll(addresses, static address => address.ToString());
        }
        catch (SocketException ex) when (ex.SocketErrorCode is SocketError.HostNotFound or SocketError.NoData)
        {
            return [];
        }
    }
}
