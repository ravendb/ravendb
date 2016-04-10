using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide;

namespace Raven.Server.Utils
{
    public class WelcomeMessage
    {
        // TODO : Take RunningOnPosix from Raven.Sparrow when Fitzhak will finish MemoryStatistics
        private static bool RunningOnPosix = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                             RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

        private static ConsoleColor DefaultLinuxBackgroundColor = ConsoleColor.DarkMagenta; // Ubuntu default background RGB=48,10,36 or #300A24

        public static void Print()
        {
            const string asciiHeader = @"        ____                       ____  _{0}       |  _ \ __ ___   _____ _ __ |  _ \| |__{0}       | |_) / _` \ \ / / _ \ '_ \| | | | '_ \{0}       |  _ < (_| |\ V /  __/ | | | |_| | |_) |{0}       |_| \_\__,_| \_/ \___|_| |_|____/|_.__/{0}{0}";
            ConsoleWriteLineWithColor(ConsoleColor.DarkGray, asciiHeader, Environment.NewLine);
            ConsoleWriteLineWithColor(ConsoleColor.Cyan, "      Safe by default, optimized for efficiency");
            Console.WriteLine();

            const string lineBorder = "+---------------------------------------------------------------+";

            ConsoleWriteLineWithColor(ConsoleColor.Yellow, " Build {0}, Version {1}, Commit {2}, PID {3}",
                ServerVersion.Build, ServerVersion.Version, ServerVersion.CommitHash, Process.GetCurrentProcess().Id);
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
                throw new ArgumentNullException("consoleTexts");
            }

            // Linux cannot and will not support getting current color : https://github.com/aspnet/dnx/issues/1708
            var previousForegroundColor = ConsoleColor.White;
            var previousBackgroundColor = DefaultLinuxBackgroundColor;
            if (RunningOnPosix == false)
            { 
                previousForegroundColor = Console.ForegroundColor;
                previousBackgroundColor = Console.BackgroundColor;
            }

            foreach (var consoleText in consoleTexts)
            {
                Console.ForegroundColor = consoleText.ForegroundColor;
                Console.BackgroundColor = consoleText.BackgroundColor;
                if (RunningOnPosix == true)
                    Console.BackgroundColor = DefaultLinuxBackgroundColor;

                Console.Write(consoleText.Message, consoleText.Args);

                if (consoleText.IsNewLinePostPended)
                {
                    Console.WriteLine();
                }
            }

            Console.ForegroundColor = previousForegroundColor;
            Console.BackgroundColor = previousBackgroundColor;
        }

        private static void ConsoleWriteLineWithColor(ConsoleColor color, string message, params object[] args)
        {
            ConsoleColor consoleBackgroundColor = DefaultLinuxBackgroundColor;
            if (RunningOnPosix == false)
                consoleBackgroundColor = Console.BackgroundColor;

            ConsoleWriteWithColor(new ConsoleText
            {
                ForegroundColor = color,
                BackgroundColor = consoleBackgroundColor,
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
                    BackgroundColor = Console.BackgroundColor;
                }
            }

            public string Message { get; set; }
            public object[] Args { get; set; }
            public ConsoleColor ForegroundColor { get; set; }
            public ConsoleColor BackgroundColor { get; set; }
            public bool IsNewLinePostPended { get; set; }
        }
    }
}