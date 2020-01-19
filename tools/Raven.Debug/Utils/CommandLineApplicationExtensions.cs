using McMaster.Extensions.CommandLineUtils;

namespace Raven.Debug.Utils
{
    internal static class CommandLineApplicationExtensions
    {
        public static int ExitWithError(this CommandLineApplication cmd, string errMsg)
        {
            cmd.Error.WriteLine(errMsg);
            cmd.ShowHelp();
            return -1;
        }
    }
}
