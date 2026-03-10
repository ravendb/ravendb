using System;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Represents the <c>today()</c> RQL function call.
    /// Returns the start of the current UTC day (midnight) on the server at query execution time.
    /// </summary>
    public sealed class TodayMethodCall : MethodCall
    {
        public static readonly TodayMethodCall Instance = new TodayMethodCall();

        private TodayMethodCall()
        {
            Args = Array.Empty<object>();
        }
    }
}
