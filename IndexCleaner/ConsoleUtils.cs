using System;

namespace Raven.IndexCleaner
{
    public static class ConsoleUtils
    {
        public static void ConsoleWriteLineWithColor(ConsoleColor color, string message, params object[] args)
        {
            var previousColor = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine(message, args);
            Console.ForegroundColor = previousColor;
        }

        public static void PrintErrorAndFail(string ErrorMessage, string stackTrace = null)
        {
            Console.Clear();
            ConsoleWriteLineWithColor(ConsoleColor.Red, ErrorMessage);
            if (stackTrace != null)
            {
                ConsoleWriteLineWithColor(ConsoleColor.Blue, "StackTrace:\n" + stackTrace);
            }
            Console.Read();

            throw new Exception(ErrorMessage);
        }
    }
}
