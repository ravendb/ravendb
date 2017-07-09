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
            RestartServer
        }

        public static string GetDelimiterKeyWord => "DELIMITER";
        public static string GetDelimiterString(Delimiter delimiter) => $"{GetDelimiterKeyWord}<{delimiter}>";        
    }
}