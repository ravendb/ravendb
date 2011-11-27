using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Rhino.Licensing
{
	public class SntpClient
	{
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
			var milliSeconds = GetMilliSeconds(sntpData, 40);
			return ComputeDate(milliSeconds);
		}

		private static DateTime ComputeDate(ulong milliseconds)
		{
			return new DateTime(1900, 1, 1).Add(TimeSpan.FromMilliseconds(milliseconds));
		}

		private static ulong GetMilliSeconds(byte[] sntpData, byte offset)
		{
			ulong intpart = 0, fractpart = 0;

			for (var i = 0; i <= 3; i++)
			{
				intpart = 256*intpart + sntpData[offset + i];
			}
			for (var i = 4; i <= 7; i++)
			{
				fractpart = 256*fractpart + sntpData[offset + i];
			}
			var milliseconds = intpart*1000 + (fractpart*1000)/0x100000000L;
			return milliseconds;
		}

		public void BeginGetDate(Action<DateTime> getTime, Action failure)
		{
			index += 1;
			if (hosts.Length <= index)
			{
				failure();
				return;
			}
			try
			{
				var host = hosts[index];
				var state = new State(null, null, getTime, failure);
				var result = Dns.BeginGetHostAddresses(host, EndGetHostAddress, state );
				RegisterWaitForTimeout(state, result);
			}
			catch (Exception)
			{
				// retry, recursion stops at the end of the hosts
				BeginGetDate(getTime, failure);
			}
		}

	    private void EndGetHostAddress(IAsyncResult asyncResult)
        {
	        var state = (State) asyncResult.AsyncState;
	        try
	        {
	            var addresses = Dns.EndGetHostAddresses(asyncResult);
	            var endPoint = new IPEndPoint(addresses[0], 123);

	            var socket = new UdpClient();
	            socket.Connect(endPoint);
	            socket.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReceiveTimeout, 500);
	            socket.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.SendTimeout, 500);
	            var sntpData = new byte[SntpDataLength];
	            sntpData[0] = 0x1B; // version = 4 & mode = 3 (client)

	        	var newState = new State(socket, endPoint, state.GetTime, state.Failure);
	        	var result = socket.BeginSend(sntpData, sntpData.Length, EndSend, newState);
				RegisterWaitForTimeout(newState, result);
	        }
	        catch (Exception)
	        {
                // retry, recursion stops at the end of the hosts
                BeginGetDate(state.GetTime, state.Failure);
	
	        }
	    }

		private void RegisterWaitForTimeout(State newState, IAsyncResult result)
		{
			if(result != null)
				ThreadPool.RegisterWaitForSingleObject(result.AsyncWaitHandle, MaybeOperationTimeout, newState, 500, true);
		}

		private void MaybeOperationTimeout(object state, bool timedOut)
		{
			if (timedOut == false)
				return;

			var theState = (State)state;
			try
			{
				theState.Socket.Close();
			}
			catch (Exception)
			{
				// retry, recursion stops at the end of the hosts
				BeginGetDate(theState.GetTime, theState.Failure);
			}
		}

		private void EndSend(IAsyncResult ar)
		{
			var state = (State) ar.AsyncState;
			try
			{
				state.Socket.EndSend(ar);
				var result = state.Socket.BeginReceive(EndReceive, state);
				RegisterWaitForTimeout(state, result);
			}
			catch
			{
				state.Socket.Close();
				BeginGetDate(state.GetTime, state.Failure);
			}
		}

		private void EndReceive(IAsyncResult ar)
		{
			var state = (State) ar.AsyncState;
			try
			{
				var endPoint = state.EndPoint;
				var sntpData = state.Socket.EndReceive(ar, ref endPoint);
				if(IsResponseValid(sntpData)==false)
				{
					state.Failure();
					return;
				}
				var transmitTimestamp = GetTransmitTimestamp(sntpData);
				state.GetTime(transmitTimestamp);
			}
			catch
			{
				BeginGetDate(state.GetTime, state.Failure);
			}
			finally
			{
				state.Socket.Close();
			}
		}

		private bool IsResponseValid(byte[] sntpData)
		{
			return sntpData.Length >= SntpDataLength && GetIsServerMode(sntpData);
		}

		#region Nested type: State

		public class State
		{
			public State(UdpClient socket, IPEndPoint endPoint, Action<DateTime> getTime, Action failure)
			{
				Socket = socket;
				EndPoint = endPoint;
				GetTime = getTime;
				Failure = failure;
			}

			public UdpClient Socket { get; private set; }

			public Action<DateTime> GetTime { get; private set; }

			public Action Failure { get; private set; }

			public IPEndPoint EndPoint { get; private set; }
		}

		#endregion
	}
}