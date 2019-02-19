using System;
using System.IO;
using System.IO.Pipes;
using System.Text;
using System.Threading;
using rvn.Utils;
using Raven.Server.Utils;
using Raven.Server.Utils.Cli;

namespace rvn
{
    internal class AdminChannel
    {
        public static void Connect(int? pid)
        {
            bool reconnect = true;
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            while (reconnect)
            {
                reconnect = false;

                if (pid == null)
                {
                    pid = ServerProcessUtil.GetRavenServerPid();
                }

                try
                {
                    var pipeName = Pipes.GetPipeName(Pipes.AdminConsolePipePrefix, pid.Value);
                    var client = new NamedPipeClientStream(pipeName);

                    try
                    {
                        client.Connect(3000);
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine(Environment.NewLine + "Couldn't connect to " + pipeName);
                        Console.ResetColor();
                        Console.WriteLine();
                        Console.WriteLine(ex);
                        Environment.Exit(100);
                    }

                    var reader = new StreamReader(client);
                    var writer = new StreamWriter(client);
                    var buffer = new char[16 * 1024];
                    var sb = new StringBuilder();

                    RavenCli.Delimiter[] delimiters =
                    {
                        RavenCli.Delimiter.NotFound,
                        RavenCli.Delimiter.ReadLine,
                        RavenCli.Delimiter.ReadKey,
                        RavenCli.Delimiter.Clear,
                        RavenCli.Delimiter.Logout,
                        RavenCli.Delimiter.Shutdown,
                        RavenCli.Delimiter.RestartServer,
                        RavenCli.Delimiter.ContinuePrinting
                    };

                    string restOfString = null;
                    while (true)
                    {
                        sb.Clear();
                        bool skipOnceRead = false;
                        if (restOfString != null)
                        {
                            sb.Append(restOfString);
                            restOfString = null;
                            skipOnceRead = true; // to avoid situation where another delimiter passed in previous Read, and next Read might blocked forever
                        }

                        var delimiter = RavenCli.Delimiter.NotFound;
                        // ReSharper disable once LoopVariableIsNeverChangedInsideLoop
                        while (delimiter == RavenCli.Delimiter.NotFound)
                        {
                            if (skipOnceRead == false)
                            {
                                var v = reader.Read(buffer, 0, 8192);
                                if (v == 0)
                                    continue;

                                sb.Append(new string(buffer, 0, v));
                            }
                            else
                            {
                                skipOnceRead = false;
                            }

                            var sbString = sb.ToString();
                            var firstDelimiterPos = sbString.IndexOf(RavenCli.DelimiterKeyWord, StringComparison.Ordinal);
                            if (firstDelimiterPos == -1)
                                continue;
                            var delimiterString = sbString.Substring(firstDelimiterPos);

                            RavenCli.Delimiter firstDelimiter = RavenCli.Delimiter.NotFound;
                            int firstIndex = 0;
                            var lowestPos = 8192;
                            foreach (var del in delimiters)
                            {
                                var index = delimiterString.IndexOf(RavenCli.GetDelimiterString(del), StringComparison.Ordinal);
                                if (index == -1)
                                    continue;
                                if (index < lowestPos)
                                {
                                    lowestPos = index;
                                    firstDelimiter = del;
                                    firstIndex = index;
                                }
                            }
                            if (firstDelimiter == RavenCli.Delimiter.NotFound)
                                continue;

                            var posAfterFirstDelimiter = firstIndex + RavenCli.GetDelimiterString(firstDelimiter).Length;
                            restOfString = delimiterString.Substring(posAfterFirstDelimiter);

                            delimiter = firstDelimiter;
                            break;
                        }

                        var str = sb.ToString();
                        Console.Write(str.Substring(0, str.IndexOf(RavenCli.DelimiterKeyWord, StringComparison.Ordinal)));

                        if (delimiter == RavenCli.Delimiter.ContinuePrinting)
                        {
                            continue;
                        }

                        switch (delimiter)
                        {
                            case RavenCli.Delimiter.ReadLine:
                                writer.WriteLine(Console.ReadLine());
                                break;
                            case RavenCli.Delimiter.ReadKey:
                                writer.Write(Console.ReadKey().KeyChar);
                                break;
                            case RavenCli.Delimiter.Clear:
                                Console.Clear();
                                break;
                            case RavenCli.Delimiter.Logout:
                            case RavenCli.Delimiter.Shutdown:
                                Console.WriteLine();
                                Environment.Exit(0);
                                break;
                            case RavenCli.Delimiter.RestartServer:
                                Console.WriteLine();
                                for (int i = 10; i >= 0; i--)
                                {
                                    Console.Write($"\rTrying to reconnect in {i} seconds ...  ");
                                    Thread.Sleep(1000);
                                }
                                Console.WriteLine();
                                reconnect = true;
                                break;
                        }
                        writer.Flush();
                        if (reconnect)
                            break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }
    }
}
