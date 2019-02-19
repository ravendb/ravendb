using System;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Server.Utils.Cli;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.Utils
{
    public class Pipes
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<Pipes>("Server");

        public const string AdminConsolePipePrefix = "raven-control-pipe-";

        public const string LogStreamPipePrefix = "raven-logs-pipe-";

        private static readonly string PipesDir = Path.Combine(Path.GetTempPath(), "ravendb-pipe");

        public static NamedPipeServerStream OpenAdminConsolePipe()
        {
            var pipeName = GetPipeName(AdminConsolePipePrefix);
            var pipe = new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous, 1024, 1024);

            return pipe;
        }

        private static string GetPipeName(string namePrefix)
        {
            using (var currentProcess = Process.GetCurrentProcess())
            {
                return GetPipeName(namePrefix, currentProcess.Id);
            }
        }

        public static string GetPipeName(string namePrefix, int pid)
        {
            var name = $"{namePrefix}{pid}";
            if (PlatformDetails.RunningOnPosix)
                name = Path.Combine(PipesDir, name);

            return name;
        }

        public static void CleanupOldPipeFiles()
        {
            if (PlatformDetails.RunningOnPosix)
            {
                DeleteOldPipeFiles(PipesDir);
            }
        }

        public static async Task ListenToAdminConsolePipe(RavenServer ravenServer, NamedPipeServerStream adminConsolePipe)
        {
            var pipe = adminConsolePipe;

            // We start the server pipe only when running as a server
            // so we won't generate one per server in our test environment 
            if (pipe == null)
                return;

            try
            {
                while (true)
                {
                    await pipe.WaitForConnectionAsync();
                    var reader = new StreamReader(pipe);
                    var writer = new StreamWriter(pipe);
                    try
                    {
                        var cli = new RavenCli();
                        var restart = cli.Start(ravenServer, writer, reader, false, true);
                        if (restart)
                        {
                            writer.WriteLine("Restarting Server...<DELIMETER_RESTART>");
                            Program.ResetServerMre.Set();
                            Program.ShutdownServerMre.Set();
                            // server restarting
                            return;
                        }

                        writer.WriteLine("Shutting Down Server...<DELIMETER_RESTART>");
                        Program.ShutdownServerMre.Set();
                        // server shutting down
                        return;
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                        {
                            Logger.Info("Got an exception inside cli (internal error) while in pipe connection", e);
                        }
                    }

                    pipe.Disconnect();
                }
            }
            catch (ObjectDisposedException)
            {
                //Server shutting down
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info("Got an exception trying to connect to server admin channel pipe", e);
                }
            }
        }

        private static void DeleteOldPipeFiles(string pipeDir)
        {
            try
            {
                if (Directory.Exists(pipeDir) == false)
                {
                    const FilePermissions mode = FilePermissions.S_IRWXU;
                    var rc = Syscall.mkdir(pipeDir, (ushort)mode);
                    if (rc != 0)
                        throw new IOException($"Unable to create directory {pipeDir} with permission {mode}. LastErr={Marshal.GetLastWin32Error()}");
                }

                var pipeFiles = Directory.GetFiles(pipeDir, AdminConsolePipePrefix + "*")
                    .Concat(Directory.GetFiles(pipeDir, LogStreamPipePrefix + "*"));
                foreach (var pipeFile in pipeFiles)
                {
                    try
                    {
                        File.Delete(pipeFile);
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Unable to delete old pipe file " + pipeFile, e);
                    }
                }
            }
            catch (Exception ex)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Unable to list old pipe files for deletion", ex);
            }
        }

        public static NamedPipeServerStream OpenLogStreamPipe()
        {
            var pipeName = GetPipeName(LogStreamPipePrefix);
            var pipe = new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1, PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous, 1024, 1024);

            return pipe;
        }

        public static async Task ListenToLogStreamPipe(RavenServer ravenServer, NamedPipeServerStream logstreamPipe)
        {
            var pipe = logstreamPipe;
            if (pipe == null)
                return;

            try
            {
                while (true)
                {
                    await pipe.WaitForConnectionAsync();

                    try
                    {
                        LoggingSource.Instance.AttachPipeSink(pipe);

                        while (pipe.IsConnected && pipe.CanWrite)
                            await Task.Delay(TimeSpan.FromSeconds(1));
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsInfoEnabled)
                            Logger.Info("Error streaming logs through pipe.", e);
                    }
                    finally
                    {
                        LoggingSource.Instance.DetachPipeSink();
                        pipe.Disconnect();
                    }
                }
            }
            catch (ObjectDisposedException)
            {
                //Server shutting down
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error connecting to log stream pipe", e);
            }
        }
    }
}
