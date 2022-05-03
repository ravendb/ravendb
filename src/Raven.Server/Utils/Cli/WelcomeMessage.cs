using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Utils;

namespace Raven.Server.Utils.Cli
{
    public class WelcomeMessage : ConsoleMessage
    {
        public const string AsciiHeader = @"       _____                       _____  ____ {0}      |  __ \                     |  __ \|  _ \ {0}      | |__) |__ ___   _____ _ __ | |  | | |_) |{0}      |  _  // _` \ \ / / _ \ '_ \| |  | |  _ < {0}      | | \ \ (_| |\ V /  __/ | | | |__| | |_) |{0}      |_|  \_\__,_| \_/ \___|_| |_|_____/|____/ {0}{0}";

        public WelcomeMessage(TextWriter tw) : base(tw)
        {
        }

        public override void Print()
        {
            ConsoleWriteLineWithColor(ConsoleColor.DarkRed, AsciiHeader, Environment.NewLine);
            ConsoleWriteLineWithColor(ConsoleColor.Cyan, "      Safe by default, optimized for efficiency");
            _tw.WriteLine();

            const string lineBorder = "+---------------------------------------------------------------+";

            using (var currentProcess = Process.GetCurrentProcess())
            {
                ConsoleWriteLineWithColor(ConsoleColor.Yellow,
                    " Build {0}, Version {1}, SemVer {2}, Commit {3}\r\n PID {4}, {5} bits, {6} Cores, Phys Mem {7}, Arch: {8}",
                    ServerVersion.Build, ServerVersion.Version, ServerVersion.FullVersion, ServerVersion.CommitHash, currentProcess.Id,
                    IntPtr.Size * 8, ProcessorInfo.ProcessorCount, MemoryInformation.TotalPhysicalMemory, RuntimeInformation.OSArchitecture);
            }
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
        private string _nodeTag;
        private string _clusterId;
        
        public ClusterMessage(TextWriter tw, string nodeTag, string clusterId) : base(tw)
        {
            _nodeTag = nodeTag;
            _clusterId = clusterId;
        }
         
        public override void Print()
        {
            if (string.IsNullOrEmpty(_clusterId))
                return;

            var clusterColor = Hashing.XXHash64.CalculateRaw(_clusterId) % (int)ConsoleColor.White + 1;//skip black
            ConsoleWriteWithColor(new ConsoleText
                {
                    Message = "Node ",
                    ForegroundColor = ConsoleColor.Gray
                },
                new ConsoleText
                {
                    Message = _nodeTag,
                    ForegroundColor = ConsoleColor.Green
                }, new ConsoleText
                {
                    Message = " in cluster ",
                    ForegroundColor = ConsoleColor.Gray
                },
                new ConsoleText
                {
                    Message = $"{_clusterId}",
                    ForegroundColor = (ConsoleColor)clusterColor
                }
                );
            _tw.WriteLine();
        }
    }
}
