using System;
using Raven.Server.ServerWide;

namespace Raven.Server.Utils
{
    public class WelcomeMessage
    {

        public static void Print()
        {
            const string asciiHeader = @"        ____                       ____  _{0}       |  _ \ __ ___   _____ _ __ |  _ \| |__{0}       | |_) / _` \ \ / / _ \ '_ \| | | | '_ \{0}       |  _ < (_| |\ V /  __/ | | | |_| | |_) |{0}       |_| \_\__,_| \_/ \___|_| |_|____/|_.__/{0}{0}";
            ConsoleWriteLineWithColor(ConsoleColor.DarkGray, asciiHeader, Environment.NewLine);
            ConsoleWriteLineWithColor(ConsoleColor.Cyan, "      Safe by default, optimized for efficiency");
            Console.WriteLine();

            const string lineBorder = "+---------------------------------------------------------------+";

            ConsoleWriteLineWithColor(ConsoleColor.Yellow, " Build {0}, Version {1}, Commit {2}", ServerVersion.Build, ServerVersion.Version, ServerVersion.CommitHash);
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

            var previousForegroundColor = Console.ForegroundColor;
            var previousBackgroundColor = Console.BackgroundColor;

            foreach (var consoleText in consoleTexts)
            {
                Console.ForegroundColor = consoleText.ForegroundColor;
                Console.BackgroundColor = consoleText.BackgroundColor;

                Console.Write(consoleText.Message, consoleText.Args);

                if (consoleText.IsNewLinePostPended)
                {
                    Console.WriteLine();
                }
            }

            Console.ForegroundColor = previousForegroundColor;
            Console.BackgroundColor = previousBackgroundColor;
        }

        private static void ConsoleWriteWithColor(ConsoleColor color, string message, params object[] args)
        {
            ConsoleWriteWithColor(new ConsoleText
            {
                ForegroundColor = color,
                BackgroundColor = Console.BackgroundColor,
                IsNewLinePostPended = false,
                Message = message,
                Args = args
            });
        }

        private static void ConsoleWriteLineWithColor(ConsoleColor color, string message, params object[] args)
        {
            ConsoleWriteWithColor(new ConsoleText
            {
                ForegroundColor = color,
                BackgroundColor = Console.BackgroundColor,
                IsNewLinePostPended = true,
                Message = message,
                Args = args
            });
        }

        internal class ConsoleText
        {
            public ConsoleText()
            {
                ForegroundColor = Console.ForegroundColor;
                BackgroundColor = Console.BackgroundColor;
            }

            public string Message { get; set; }
            public object[] Args { get; set; }
            public ConsoleColor ForegroundColor { get; set; }
            public ConsoleColor BackgroundColor { get; set; }
            public bool IsNewLinePostPended { get; set; }
        }
    }
}