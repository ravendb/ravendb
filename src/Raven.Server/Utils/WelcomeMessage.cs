using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide;
using Sparrow.LowMemory;
using Sparrow.Utils;
using static Sparrow.Platform.PlatformDetails;

namespace Raven.Server.Utils
{
    public class WelcomeMessage
    {
        public WelcomeMessage(TextWriter tw)
        {
            _tw = tw;
        }

        private readonly TextWriter _tw;
        private bool WithColoring() => _tw == Console.Out;

        public void Print()
        {
            const string asciiHeader = @"       _____                       _____  ____ {0}      |  __ \                     |  __ \|  _ \ {0}      | |__) |__ ___   _____ _ __ | |  | | |_) |{0}      |  _  // _` \ \ / / _ \ '_ \| |  | |  _ < {0}      | | \ \ (_| |\ V /  __/ | | | |__| | |_) |{0}      |_|  \_\__,_| \_/ \___|_| |_|_____/|____/ {0}{0}";
            ConsoleWriteLineWithColor(ConsoleColor.DarkRed, asciiHeader, Environment.NewLine);
            ConsoleWriteLineWithColor(ConsoleColor.Cyan, "      Safe by default, optimized for efficiency");
            _tw.WriteLine();

            const string lineBorder = "+---------------------------------------------------------------+";

            var meminfo = MemoryInformation.GetMemoryInfo();
            ConsoleWriteLineWithColor(ConsoleColor.Yellow, " Build {0}, Version {1}, SemVer {2}, Commit {3}\r\n PID {4}, {5} bits, {6} Cores, Phys Mem {7}, Arch: {8}",
                ServerVersion.Build, ServerVersion.Version, ServerVersion.FullVersion, ServerVersion.CommitHash, Process.GetCurrentProcess().Id,
                IntPtr.Size * 8, ProcessorInfo.ProcessorCount, meminfo.TotalPhysicalMemory, RuntimeInformation.OSArchitecture);
            ConsoleWriteLineWithColor(ConsoleColor.DarkCyan, " Source Code (git repo): https://github.com/ravendb/ravendb");
            ConsoleWriteWithColor(new ConsoleText { Message = " Built with ", ForegroundColor = ConsoleColor.Gray },
                new ConsoleText { Message = "love ", ForegroundColor = ConsoleColor.Red },
                new ConsoleText { Message = "by ", ForegroundColor = ConsoleColor.Gray },
                new ConsoleText { Message = "Hibernating Rhinos ", ForegroundColor = ConsoleColor.Yellow },
                new ConsoleText { Message = "and awesome contributors!", ForegroundColor = ConsoleColor.Gray, IsNewLinePostPended = true });
            _tw.WriteLine(lineBorder);
        }

        private void ConsoleWriteWithColor(params ConsoleText[] consoleTexts)
        {
            if (consoleTexts == null)
            {
                throw new ArgumentNullException(nameof(consoleTexts));
            }

            // Linux cannot and will not support getting current color : https://github.com/aspnet/dnx/issues/1708
            var previousForegroundColor = ConsoleColor.White;
            if (RunningOnPosix == false)
            {
                if (WithColoring())
                    previousForegroundColor = Console.ForegroundColor;
            }

            foreach (var consoleText in consoleTexts)
            {
                if (WithColoring())
                    Console.ForegroundColor = consoleText.ForegroundColor;

                if (consoleText.Args != null)
                    _tw.Write(consoleText.Message, consoleText.Args);
                else
                    _tw.Write(consoleText.Message);

                if (consoleText.IsNewLinePostPended)
                {
                    _tw.WriteLine();
                }
            }

            if (WithColoring())
                Console.ForegroundColor = previousForegroundColor;
        }

        private void ConsoleWriteLineWithColor(ConsoleColor color, string message, params object[] args)
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