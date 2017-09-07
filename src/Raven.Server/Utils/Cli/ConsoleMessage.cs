using System;
using System.IO;
using Sparrow.Platform;

namespace Raven.Server.Utils.Cli
{
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
            if (PlatformDetails.RunningOnPosix == false)
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
                if (PlatformDetails.RunningOnPosix == false)
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
