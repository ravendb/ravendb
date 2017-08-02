using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Utils;
using static Sparrow.Platform.PlatformDetails;

namespace Raven.Server.Utils
{
    public class WelcomeMessage : ConsoleMessage
    {
        public WelcomeMessage(TextWriter tw) : base(tw)
        {
        }

        public override void Print()
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
    }

    public class ClusterMessage : ConsoleMessage
    {
        private readonly ServerStore _server;

        public ClusterMessage(TextWriter tw, ServerStore server) : base(tw)
        {
            _server = server;
        }
         
        public override void Print()
        {
            if (string.IsNullOrEmpty(_server.Engine.ClusterId))
                return;
            var nodeTag = _server.Engine.Tag;
            var id = _server.Engine.ClusterId;
            var clusterColor = Hashing.XXHash64.CalculateRaw(id) % (int)ConsoleColor.White + 1;//skip black
            ConsoleWriteWithColor(new ConsoleText
                {
                    Message = "Node ",
                    ForegroundColor = ConsoleColor.Gray
                },
                new ConsoleText
                {
                    Message = nodeTag,
                    ForegroundColor = ConsoleColor.Green
                }, new ConsoleText
                {
                    Message = " in cluster ",
                    ForegroundColor = ConsoleColor.Gray
                },
                new ConsoleText
                {
                    Message = $"{id}",
                    ForegroundColor = (ConsoleColor)clusterColor
                }
                );
            _tw.WriteLine();
        }
    }

    public abstract class ConsoleMessage
    {
        protected ConsoleMessage(TextWriter tw)
        {
            _tw = tw;
        }

        protected readonly TextWriter _tw;
        private bool WithColoring() => _tw == Console.Out;

        public abstract void Print();

        protected void ConsoleWriteWithColor(params ConsoleText[] consoleTexts)
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

        protected void ConsoleWriteLineWithColor(ConsoleColor color, string message, params object[] args)
        {
            ConsoleWriteWithColor(new ConsoleText
            {
                ForegroundColor = color,
                IsNewLinePostPended = true,
                Message = message,
                Args = args
            });
        }

        protected class ConsoleText
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