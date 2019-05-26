using System;

namespace Raven.Client.Util
{
    internal static class RaftIdGenerator
    {
        public static string NewId()
        {
            return Guid.NewGuid().ToString();
        }

        // if the don't care id is used it may cause that on retry/resend of the command we will end up in double applying of the command (once for the original request and for the retry).
        public static string DontCareId => string.Empty;
    }
}
