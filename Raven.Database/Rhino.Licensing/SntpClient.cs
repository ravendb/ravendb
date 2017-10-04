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

        public static readonly byte SntpDataLength = 48;
        private readonly string[] hosts;
        private int index = 0;

        public SntpClient(string[] hosts)
        {
            this.hosts = hosts;
        }

        private static bool GetIsServerMode(byte[] sntpData)
        {
            return (sntpData[0] & 0x7) == 4 /* server mode */;
        }

        public static DateTime GetTransmitTimestamp(byte[] sntpData)
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
                    index = (index + 1) % hosts.Length;
                    throw new TimeoutException("After " + sp.Elapsed + " we couldn't get a time from the network, giving up (tried " + (index + 1) + " servers");
                }
                if (hosts.Length <= index)
                {
                    index = 0;
                    throw new InvalidOperationException(
                        "After trying out all the hosts, was unable to find anyone that could tell us what the time is");
                }

                var host = hosts[index];
                var hostTiming = Stopwatch.StartNew();

                var exceptionWasThrown = false;
                try
                {
                    var addresses = await Dns.GetHostAddressesAsync(host).ConfigureAwait(false);
                    var endPoint = new IPEndPoint(addresses[0], 123);

                    if (log.IsDebugEnabled)
                        log.Debug("Requesting timing information from {0}", host);
                    using (var udpClient = new UdpClient())
                    {
                        udpClient.Connect(endPoint);
                        //Socket timeouts are only been respected by sync calls, i leave this comment so 
                        //nobody will try to use timeouts in the future.
                        //udpClient.Client.ReceiveTimeout = 500;
                        //udpClient.Client.SendTimeout = 500;
                        var sntpData = new byte[SntpDataLength];
                        sntpData[0] = 0x1B; // version = 4 & mode = 3 (client)

                        try
                        {
                            var sendTask = udpClient.SendAsync(sntpData, sntpData.Length);
                            if (await Task.WhenAny(sendTask, Task.Delay(TimeSpan.FromMilliseconds(500))).ConfigureAwait(false) != sendTask)
                            {
                                throw new TimeoutException("Failed to send data to " + host+ "within 500ms");
                            }
                        }
                        catch (Exception e)
                        {
                            exceptionWasThrown = true;

                            if (log.IsDebugEnabled)
                                log.DebugException("Could not send time request to : " + host + " took " + hostTiming.Elapsed, e);
                        }

                        if (exceptionWasThrown)
                        {
                            index++;
                            continue;
                        }

                        try
                        {
                            var receiveTask = udpClient.ReceiveAsync();
                            if (await Task.WhenAny(receiveTask, Task.Delay(TimeSpan.FromMilliseconds(500))).ConfigureAwait(false) != receiveTask)
                            {
                                //The purpose of this task is just to observe and log the exception from the receiveTask
                                //so this will not remain in the unobserved exceptions list, we do not wait for it by design.
                                #pragma warning disable 4014
                                receiveTask.ContinueWith(t =>
                                 {
                                     if (t.Exception != null && log.IsDebugEnabled)
                                         log.DebugException($"Got an error while trying to get the time from: ({host})", t.Exception);
                                 });
                                #pragma warning restore 4014
                                throw new TimeoutException("Failed to receive data to " + host + "within 500ms");
                            }
                            var result = receiveTask.Result;
                            hostTiming.Stop();
                            if (IsResponseValid(result.Buffer) == false)
                            {
                                if (log.IsDebugEnabled)
                                    log.Debug("Did not get valid time information from " + host + " took " + hostTiming.Elapsed);
                                index++;
                                continue;
                            }
                            var transmitTimestamp = GetTransmitTimestamp(result.Buffer);
                            if (log.IsDebugEnabled)
                            {
                                log.Debug("Got time {0} from {1} in {2}", transmitTimestamp, host, hostTiming.Elapsed);
                            }
                            return transmitTimestamp;
                        }
                        catch (Exception e)
                        {
                            if (log.IsDebugEnabled)
                                log.DebugException("Could not get time response from: " + host + " took " + hostTiming.Elapsed, e);
                            index++;
                        }
                    }
                }
                catch (Exception e)
                {
                    if (log.IsDebugEnabled)
                        log.DebugException("Could not get time from: " + host + " took " + hostTiming.Elapsed, e);
                    index++;
                }
            }
        }

        public static bool IsResponseValid(byte[] sntpData)
        {
            return sntpData.Length >= SntpDataLength && GetIsServerMode(sntpData);
        }
    }
}
