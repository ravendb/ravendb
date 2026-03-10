using System;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Represents the <c>now()</c> RQL function call.
    /// Returns the current UTC date and time on the server at query execution time.
    /// </summary>
    public sealed class NowMethodCall : MethodCall
    {
        public static readonly NowMethodCall Instance = new NowMethodCall();

        private NowMethodCall()
        {
            Args = Array.Empty<object>();
        }
    }
}
