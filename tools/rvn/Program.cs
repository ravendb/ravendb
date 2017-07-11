using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Server;
using static Sparrow.CliDelimiter;

namespace rvn
{
    class Program
    {
        static void Main(string[] args)
        {
            bool reconnect = true;
            while (reconnect)
            {
                reconnect = false;
                if (args.Length < 1 || !args[0].ToLower().Equals("admin-channel"))
                {
                    Console.WriteLine("Usage : rvn admin-channel [PID]" + Environment.NewLine);
                    Console.Out.Flush();
                    Environment.Exit(5);
                }
                int pid = 0;
                if (args.Length > 2)
                {
                    Console.WriteLine("Invalid number of arges passed with admin-channgel" + Environment.NewLine +
                                      "Usage: admin-channel [RavenDB PID]" + Environment.NewLine);
                    Console.Out.Flush();
                    Environment.Exit(2);
                }
                if (args.Length == 2)
                    pid = Convert.ToInt32(args[1]);
                else
                {
                    var processes = Process.GetProcessesByName("Raven.Server");
                    var thisProcess = Process.GetCurrentProcess();
                    var availableDotnetProcesses = new List<Process>();
                    foreach (var pr in processes)
                    {
                        if (thisProcess.Id == pr.Id)
                            continue;
                        availableDotnetProcesses.Add(pr);
                    }

                    if (availableDotnetProcesses.Count == 0)
                    {
                        Console.WriteLine("Couldn't find automatically another dotnet process." + Environment.NewLine +
                                          "Please specify RavenDB Server proccess manually");
                        Console.Out.Flush();
                        Environment.Exit(3);
                    }
                    else if (availableDotnetProcesses.Count == 1)
                    {
                        Console.WriteLine("Will try to connect to discovered dotnet process : " + availableDotnetProcesses.First().Id + "..." + Environment.NewLine);
                        Console.Out.Flush();
                        pid = availableDotnetProcesses.First().Id;
                    }
                    else
                    {
                        Console.Write("More then one dotnet process where found:");
                        availableDotnetProcesses.ForEach(x => Console.Write(" " + x.Id));
                        Console.WriteLine(Environment.NewLine + "Please specify RavenDB Server proccess manually" + Environment.NewLine);
                        Console.Out.Flush();
                        Environment.Exit(4);
                    }
                }
                try
                {
                    var pipeName = RavenServer.PipePrefix + pid;
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
                        Environment.Exit(1);
                    }

                    var reader = new StreamReader(client);
                    var writer = new StreamWriter(client);
                    var buffer = new char[16 * 1024];
                    var sb = new StringBuilder();

                    Delimiter[] delimiters =
                    {
                        Delimiter.NotFound,
                        Delimiter.ReadLine,
                        Delimiter.ReadKey,
                        Delimiter.Clear,
                        Delimiter.Logout,
                        Delimiter.Shutdown,
                        Delimiter.RestartServer,
                        Delimiter.ContinuePrinting
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

                        var delimiter = Delimiter.NotFound;
                        // ReSharper disable once LoopVariableIsNeverChangedInsideLoop
                        while (delimiter == Delimiter.NotFound)
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
                            var firstDelimiterPos = sbString.IndexOf(DelimiterKeyWord, StringComparison.Ordinal);
                            if (firstDelimiterPos == -1)
                                continue;
                            var delimiterString = sbString.Substring(firstDelimiterPos);

                            Delimiter firstDelimiter = Delimiter.NotFound;
                            int firstIndex = 0;
                            var lowestPos = 8192;
                            foreach (var del in delimiters)
                            {
                                var index = delimiterString.IndexOf(GetDelimiterString(del), StringComparison.Ordinal);
                                if (index == -1)
                                    continue;
                                if (index < lowestPos)
                                {
                                    lowestPos = index;
                                    firstDelimiter = del;
                                    firstIndex = index;
                                }
                            }
                            if (firstDelimiter == Delimiter.NotFound)
                                continue;

                            var posAgterFirstDelimiter = firstIndex + GetDelimiterString(firstDelimiter).Length;
                            restOfString = delimiterString.Substring(posAgterFirstDelimiter);

                            delimiter = firstDelimiter;
                            break;
                        }

                        var str = sb.ToString();
                        Console.Write(str.Substring(0, str.IndexOf(DelimiterKeyWord, StringComparison.Ordinal)));

                        if (delimiter == Delimiter.ContinuePrinting)
                        {
                            continue;
                        }

                        switch (delimiter)
                        {
                            case Delimiter.ReadLine:
                                writer.WriteLine(Console.ReadLine());
                                break;
                            case Delimiter.ReadKey:
                                writer.Write(Console.ReadKey().KeyChar);
                                break;
                            case Delimiter.Clear:
                                Console.Clear();
                                break;
                            case Delimiter.Logout:
                            case Delimiter.Shutdown:
                                Console.WriteLine();
                                Environment.Exit(0);
                                break;
                            case Delimiter.RestartServer:
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

