using System;
using System.Collections.Generic;
using System.Text;

namespace Sparrow.Utils
{
    public static class Dos2Linux
    {
        public static string String(string str)
        {
            if (Platform.PlatformDetails.RunningOnPosix == true)
                return str.Replace("\r\n", "\n");
            return str;
        }
    }
}
