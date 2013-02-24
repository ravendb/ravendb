using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;

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

		public Task<DateTime> GetDateAsync()
		{
			index++;
			if (hosts.Length <= index)
			{
				throw new InvalidOperationException(
					"After trying out all the hosts, was unable to find anyone that could tell us what the time is");
			}
			var host = hosts[index];
			return Task.Factory.FromAsync<IPAddress[]>((callback, state) => Dns.BeginGetHostAddresses(host, callback, state),
												Dns.EndGetHostAddresses, host)
				.ContinueWith(hostTask =>
				{
					if (hostTask.IsFaulted)
					{
						log.DebugException("Could not get time from: " + host, hostTask.Exception);
						return GetDateAsync();
					}
					var endPoint = new IPEndPoint(hostTask.Result[0], 123);


					var socket = new UdpClient();
					socket.Connect(endPoint);
					socket.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, 500);
					socket.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout, 500);
					var sntpData = new byte[SntpDataLength];
					sntpData[0] = 0x1B; // version = 4 & mode = 3 (client)
					return Task.Factory.FromAsync<int>(
						(callback, state) => socket.BeginSend(sntpData, sntpData.Length, callback, state),
						socket.EndSend, null)
							   .ContinueWith(sendTask =>
							   {
								   if (sendTask.IsFaulted)
								   {
									   try
									   {
										   socket.Close();
									   }
									   catch (Exception)
									   {
									   }
									   log.DebugException("Could not send time request to : " + host, sendTask.Exception);
									   return GetDateAsync();
								   }

								   return Task.Factory.FromAsync<byte[]>(socket.BeginReceive, (ar) => socket.EndReceive(ar, ref endPoint), null)
								              .ContinueWith(receiveTask =>
								              {
									              if (receiveTask.IsFaulted)
									              {
										              try
										              {
											              socket.Close();
										              }
										              catch (Exception)
										              {
										              }
										              log.DebugException("Could not get time response from: " + host, receiveTask.Exception);
										              return GetDateAsync();
									              }
									              var result = receiveTask.Result;
									              if (IsResponseValid(result) == false)
									              {
										              log.Debug("Did not get valid time information from " + host);
										              return GetDateAsync();
									              }
									              var transmitTimestamp = GetTransmitTimestamp(result);
									              return new CompletedTask<DateTime>(transmitTimestamp);
								              }).Unwrap();
							   }).Unwrap();
				}).Unwrap();
		}

		private bool IsResponseValid(byte[] sntpData)
		{
			return sntpData.Length >= SntpDataLength && GetIsServerMode(sntpData);
		}
	}
}