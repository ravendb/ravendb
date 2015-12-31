using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

using Raven.Abstractions.Logging;

namespace Rhino.Licensing
{
    public class SntpClient
    {
        private ILog log = LogManager.GetCurrentClassLogger();

        private const byte SntpDataLength = 48;
        private readonly string[] hosts;
        private int index = -1;

        public SntpClient(string[] hosts)
        {
            this.hosts = hosts;
        }

        private static bool GetIsServerMode(byte[] sntpData)
        {
            return (sntpData[0] & 0x7) == 4 /* server mode */;
        }

        private static DateTime GetTransmitTimestamp(byte[] sntpData)
        {
            var milliseconds = GetMilliseconds(sntpData, 40);
            return ComputeDate(milliseconds);
        }

        private static DateTime ComputeDate(ulong milliseconds)
        {
            return new DateTime(1900, 1, 1).Add(TimeSpan.FromMilliseconds(milliseconds));
        }

        private static ulong GetMilliseconds(byte[] sntpData, byte offset)
        {
            ulong intpart = 0, fractpart = 0;

            for (var i = 0; i <= 3; i++)
            {
                intpart = 256 * intpart + sntpData[offset + i];
            }
            for (var i = 4; i <= 7; i++)
            {
                fractpart = 256 * fractpart + sntpData[offset + i];
            }
            var milliseconds = intpart * 1000 + (fractpart * 1000) / 0x100000000L;
            return milliseconds;
        }

        public async Task<DateTime> GetDateAsync()
        {
            var sp = Stopwatch.StartNew();
            while (true)
            {
                if (sp.Elapsed.TotalSeconds > 5)
                {
                    throw new TimeoutException("After " + sp.Elapsed + " we couldn't get a time from the network, giving up (tried " + (index + 1) + " servers");
                }
                index++;
                if (hosts.Length <= index)
                {
                    index = 0;
                    throw new InvalidOperationException(
                        "After trying out all the hosts, was unable to find anyone that could tell us what the time is");
                }
                var host = hosts[index];

                var exceptionWasThrown = false;

                try
                {
                    var addresses = await Dns.GetHostAddressesAsync(host).ConfigureAwait(false);
                    var endPoint = new IPEndPoint(addresses[0], 123);

                    var socket = new UdpClient();
                    try
                    {
                        socket.Connect(endPoint);
                        socket.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, 500);
                        socket.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout, 500);
                        var sntpData = new byte[SntpDataLength];
                        sntpData[0] = 0x1B; // version = 4 & mode = 3 (client)

                        try
                        {
                            await socket.SendAsync(sntpData, sntpData.Length).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            exceptionWasThrown = true;

                            if (log.IsDebugEnabled)
                                log.DebugException("Could not send time request to : " + host, e);
                        }

                        if (exceptionWasThrown)
                            continue;

                        try
                        {
                            var result = await socket.ReceiveAsync().ConfigureAwait(false);
                            if (IsResponseValid(result.Buffer) == false)
                            {
                                if (log.IsDebugEnabled)
                                    log.Debug("Did not get valid time information from " + host);
                                continue;
                            }
                            var transmitTimestamp = GetTransmitTimestamp(result.Buffer);
                            if (log.IsDebugEnabled)
                            {
                                log.Debug("Got time {0} from {1}", transmitTimestamp, host);
                            }
                            return transmitTimestamp;
                        }
                        catch (Exception e)
                        {
                            if (log.IsDebugEnabled)
                                log.DebugException("Could not get time response from: " + host, e);
                        }
                    }
                    finally
                    {
                        try
                        {
                            socket.Close();
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
                catch (Exception e)
                {
                    if (log.IsDebugEnabled)
                        log.DebugException("Could not get time from: " + host, e);
                }
            }
        }

        private bool IsResponseValid(byte[] sntpData)
        {
            return sntpData.Length >= SntpDataLength && GetIsServerMode(sntpData);
        }
    }
}
