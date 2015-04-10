using System;

namespace Raven.Server
{
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