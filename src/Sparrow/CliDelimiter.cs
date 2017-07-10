namespace Sparrow
{
    public class CliDelimiter
    {
        public enum Delimiter
        {
            NotFound,
            ReadLine,
            ReadKey,
            Clear,
            Logout,
            Quit,
            RestartServer,
            ContinuePrinting
        }

        public const string DelimiterKeyWord = "DELIMITER:";
        public static string GetDelimiterString(Delimiter delimiter) => DelimiterKeyWord + delimiter;        
    }
}