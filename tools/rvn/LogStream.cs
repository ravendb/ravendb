using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipes;
using System.Net.WebSockets;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using rvn.Utils;
using Raven.Server.Utils;
using Raven.Server.Utils.Cli;
using Sparrow.Platform;

namespace rvn
{
    public class LogStream : IDisposable
    {
        private readonly int _pid;

        private NamedPipeClientStream _client;

        private Timer _serverAliveCheck;

        private int _exitCode;

        private const int ServerAliveCheckInterval = 5000;

        public LogStream(int? pid = null)
        {
            _pid = pid ?? ServerProcessUtil.GetRavenServerPid();
        }

        public void Stop()
        {
            Dispose();
            Console.WriteLine("Stop tailing logs. Exiting..");
            Environment.Exit(_exitCode);
        }

        public async Task Connect()
        {
            _serverAliveCheck = new Timer((s) =>
            {
                if (ServerProcessUtil.IsServerDown(_pid) == false)
                    return;

                Console.WriteLine($"RavenDB server process PID {_pid} exited.");
                _exitCode = 1;
                Stop();
            }, null, 0, ServerAliveCheckInterval);
            
            try
            {
                var pipeName = Pipes.GetPipeName(Pipes.LogStreamPipePrefix, _pid);
                _client = new NamedPipeClientStream(pipeName);
                WorkaroundSetPipePathForPosix(_client, pipeName);
                try
                {
                    _client.Connect(3000);
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(Environment.NewLine + "Couldn't connect to " + pipeName);
                    Console.ResetColor();
                    Console.WriteLine();
                    Console.WriteLine(ex);
                    Environment.Exit(2);
                }

                Console.WriteLine("Connected to RavenDB server. Tailing logs...");

                var reader = new StreamReader(_client);
                var buffer = new char[8192];
                var stdOut = Console.Out;
                while (true)
                {
                    var readCount = await reader.ReadAsync(buffer, 0, buffer.Length);
                    if (readCount > 0)
                    {
                        await stdOut.WriteAsync(buffer, 0, readCount);
                    }
                }
            }
            catch (ObjectDisposedException)
            {
                // closing
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        private static void WorkaroundSetPipePathForPosix(NamedPipeClientStream client, string pipeName)
        {
            if (PlatformDetails.RunningOnPosix) // TODO: remove this if and after https://github.com/dotnet/corefx/issues/22141 (both in RavenServer.cs and AdminChannel.cs)
            {
                var pathField = client.GetType().GetField("_normalizedPipePath", BindingFlags.NonPublic | BindingFlags.Instance);
                if (pathField == null)
                {
                    throw new InvalidOperationException("Unable to set the proper path for the admin pipe, admin channel will not be available");
                }
                var pipeDir = Path.Combine(Path.GetTempPath(), "ravendb-pipe");
                pathField.SetValue(client, Path.Combine(pipeDir, pipeName));
            }
        }

        public void Dispose()
        {
            _client?.Dispose();
            _serverAliveCheck?.Dispose();

            _client = null;
            _serverAliveCheck = null;
        }
    }
}
