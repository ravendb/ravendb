using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Raven.Tests.Common;
using Rhino.Licensing;
using Xunit;

namespace Raven.Tests.Issues
{
    public class NtpServers : RavenTest
    {
        [Fact]
        public async Task CanConnectToAllNtpServers()
        {
            foreach (var host in AbstractLicenseValidator.StandardTimeServer)
            {
                var hostTiming = Stopwatch.StartNew();

                var exceptionWasThrown = false;

                var addresses = await Dns.GetHostAddressesAsync(host).ConfigureAwait(false);
                var endPoint = new IPEndPoint(addresses[0], 123);

                using (var udpClient = new UdpClient())
                {
                    udpClient.Connect(endPoint);
                    var sntpData = new byte[SntpClient.SntpDataLength];
                    sntpData[0] = 0x1B; // version = 4 & mode = 3 (client)

                    var sendTask = udpClient.SendAsync(sntpData, sntpData.Length);
                    if (await Task.WhenAny(sendTask, Task.Delay(TimeSpan.FromMilliseconds(500))).ConfigureAwait(false) != sendTask)
                    {
                        throw new TimeoutException("Failed to send data to " + host + "within 500ms");
                    }



                    var receiveTask = udpClient.ReceiveAsync();
                    if (await Task.WhenAny(receiveTask, Task.Delay(TimeSpan.FromMilliseconds(500))).ConfigureAwait(false) != receiveTask)
                    {
                        throw new TimeoutException("Failed to receive data to " + host + " within 500ms");
                    }
                    var result = receiveTask.Result;
                    hostTiming.Stop();
                    if (SntpClient.IsResponseValid(result.Buffer) == false)
                    {
                        throw new BadRequestException("Did not get valid time information from " + host + " took " + hostTiming.Elapsed);
                    }
                    var transmitTimestamp = SntpClient.GetTransmitTimestamp(result.Buffer);
                    //The idea is not to validate our own clock but to make sure we didn't get garbage.
                    Assert.True((DateTime.UtcNow - transmitTimestamp).Duration() < TimeSpan.FromHours(1));
                }
            }
        }
    }
}
