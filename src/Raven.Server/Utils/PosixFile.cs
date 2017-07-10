using System.IO;
using Sparrow.Platform;

namespace Raven.Server.Utils
{
    public class PosixFile
    {
        public static void DeleteOnClose(string file)
        {
            // On Linux we don't get DeleteOnClose
            if (PlatformDetails.RunningOnPosix == false)
                return;

            try
            {
                File.Delete(file);
            }
            catch
            {
                // ignore, nothing we can do here
            }
        }
    }
}