using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public class WelcomeMessage
    {
        // TODO : Take RunningOnPosix from Raven.Sparrow when Fitzhak will finish MemoryStatistics
        private static bool RunningOnPosix = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                             RuntimeInformation.IsOSPlatform(OSPlatform.OSX);


        public static void Print()
        {
            const string asciiHeader = @"       _____                       _____  ____ {0}      |  __ \                     |  __ \|  _ \ {0}      | |__) |__ ___   _____ _ __ | |  | | |_) |{0}      |  _  // _` \ \ / / _ \ '_ \| |  | |  _ < {0}      | | \ \ (_| |\ V /  __/ | | | |__| | |_) |{0}      |_|  \_\__,_| \_/ \___|_| |_|_____/|____/ {0}{0}";
            ConsoleWriteLineWithColor(ConsoleColor.DarkRed, asciiHeader, Environment.NewLine);
            ConsoleWriteLineWithColor(ConsoleColor.Cyan, "      Safe by default, optimized for efficiency");
            Console.WriteLine();

            const string lineBorder = "+---------------------------------------------------------------+";

            var meminfo = MemoryInformation.GetMemoryInfo();
            ConsoleWriteLineWithColor(ConsoleColor.Yellow, " Build {0}, Version {1}, SemVer {2}, Commit {3}\r\n PID {4}, {5} bits, {6} Cores, Phys Mem {7}",
                ServerVersion.Build, ServerVersion.Version, ServerVersion.FullVersion ,ServerVersion.CommitHash, Process.GetCurrentProcess().Id,
                IntPtr.Size * 8, ProcessorInfo.ProcessorCount, meminfo.TotalPhysicalMemory, meminfo.AvailableMemory);
            ConsoleWriteLineWithColor(ConsoleColor.DarkCyan, " Source Code (git repo): https://github.com/ravendb/ravendb");
            ConsoleWriteWithColor(new ConsoleText { Message = " Built with ", ForegroundColor = ConsoleColor.Gray },
                new ConsoleText { Message = "love ", ForegroundColor = ConsoleColor.Red },
                new ConsoleText { Message = "by ", ForegroundColor = ConsoleColor.Gray },
                new ConsoleText { Message = "Hibernating Rhinos ", ForegroundColor = ConsoleColor.Yellow },
                new ConsoleText { Message = "and awesome contributors!", ForegroundColor = ConsoleColor.Gray, IsNewLinePostPended = true });
            Console.WriteLine(lineBorder);
            
        }

        private static void ConsoleWriteWithColor(params ConsoleText[] consoleTexts)
        {
            if (consoleTexts == null)
            {
                throw new ArgumentNullException(nameof(consoleTexts));
            }

            // Linux cannot and will not support getting current color : https://github.com/aspnet/dnx/issues/1708
            var previousForegroundColor = ConsoleColor.White;
            if (RunningOnPosix == false)
            { 
                previousForegroundColor = Console.ForegroundColor;
            }

            foreach (var consoleText in consoleTexts)
            {
                Console.ForegroundColor = consoleText.ForegroundColor;

                Console.Write(consoleText.Message, consoleText.Args);

                if (consoleText.IsNewLinePostPended)
                {
                    Console.WriteLine();
                }
            }

            Console.ForegroundColor = previousForegroundColor;
        }

        private static void ConsoleWriteLineWithColor(ConsoleColor color, string message, params object[] args)
        {

            ConsoleWriteWithColor(new ConsoleText
            {
                ForegroundColor = color,
                IsNewLinePostPended = true,
                Message = message,
                Args = args
            });
        }

        internal class ConsoleText
        {
            public ConsoleText()
            {
                if (RunningOnPosix == false)
                {
                    ForegroundColor = Console.ForegroundColor;
                }
            }

            public string Message { get; set; }
            public object[] Args { get; set; }
            public ConsoleColor ForegroundColor { get; set; }
            public bool IsNewLinePostPended { get; set; }
        }
    }
}