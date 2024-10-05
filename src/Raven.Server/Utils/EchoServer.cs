using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Utils;

internal static class EchoServer
{
    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer(typeof(EchoServer));

    public static void StartEchoSockets(int? echoSocketPort)
    {
        if (echoSocketPort == null)
            return;

        int port = echoSocketPort.Value;
        try
        {
            _ = EchoSocketAsync(port);
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
            {
                Logger.Info("Failed to start async echo socket on " + port, e);
            }
        }

        try
        {
            new Thread(EchoSocketSync) { IsBackground = true, Name = "Sync echo socket listener", }.Start(port + 1);
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
            {
                Logger.Info("Failed to start sync echo socket on " + port + 1, e);
            }
        }
    }

    private static async Task EchoSocketAsync(int port)
    {
        TcpListener listener = null;
        try
        {
            listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            while (true)
            {
                var s = await listener.AcceptSocketAsync();
                _ = EchoAsync(s);
            }
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
            {
                Logger.Info("Error in listening to echo socket on " + port, e);
            }
        }
        finally
        {
            try
            {
                listener?.Stop();
            }
            catch
            {
                // nothing to do
            }
        }
    }

    private static void EchoSocketSync(object o)
    {
        TcpListener listener = null;
        try
        {
            int port = (int)o;
            listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            while (true)
            {
                var s = listener.AcceptSocket();
                new Thread(EchoSync) { Name = "Sync echo socket thread for: " + s.RemoteEndPoint, IsBackground = true }.Start(s);
            }
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
            {
                Logger.Info("Error in listening to echo socket on " + o, e);
            }
        }
        finally
        {
            try
            {
                listener?.Stop();
            }
            catch
            {
                // nothing to do
            }
        }
    }

    private static async Task EchoAsync(Socket s)
    {
        try
        {
            CreateLogfile(s.RemoteEndPoint);
            ArraySegment<byte> buffer = new byte[1024];
            while (true)
            {
                var read = await s.ReceiveAsync(buffer, SocketFlags.None);
                if (read == 0)
                {
                    Logger.Info($"Received 0 bytes. Close the connection. RemoteEndPoint:{s.RemoteEndPoint} SocketHashCode{s.GetHashCode()}");
                    return;
                }

                await s.SendAsync(buffer.Slice(0, read), SocketFlags.None);
            }
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
            {
                Logger.Info("Error in async echo socket", e);
            }
        }
        finally
        {
            try
            {
                s?.Dispose();
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info("Error in disposing async echo socket", e);
                }
            }
        }
    }

    private static void EchoSync(object o)
    {
        Socket s = null;
        try
        {
            s = (Socket)o;
            CreateLogfile(s.RemoteEndPoint);
            var buffer = new byte[1024];
            while (true)
            {
                var read = s.Receive(buffer);
                if (read == 0)
                {
                    Logger.Info($"Received 0 bytes. Close the connection. RemoteEndPoint:{s.RemoteEndPoint} SocketHashCode{s.GetHashCode()}");
                    return;
                }
                s.Send(buffer, 0, read, SocketFlags.None);
            }
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
            {
                Logger.Info("Error in sync echo socket", e);
            }
        }
        finally
        {
            try
            {
                s?.Dispose();
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info("Error in disposing sync echo socket", e);
                }
            }
        }
    }

    private static void CreateLogfile(EndPoint remote)
    {
        try
        {
            var f = Path.Combine(Path.GetTempPath(), "Echo_Server_" + remote + "_" + Guid.NewGuid());
            File.Create(f).Close();
        }
        catch
        {
        }

    }
}
