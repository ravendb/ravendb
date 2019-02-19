using System;
using System.IO;
using System.IO.Pipes;
using System.Threading;
using System.Threading.Tasks;
using rvn.Utils;
using Raven.Server.Utils;

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

        public void Dispose()
        {
            _client?.Dispose();
            _serverAliveCheck?.Dispose();

            _client = null;
            _serverAliveCheck = null;
        }
    }
}
