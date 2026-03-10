using System;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Represents temporal RQL function calls: <c>now()</c> and <c>today()</c>.
    /// Use <see cref="Now"/> for the current UTC date and time, or <see cref="Today"/> for the start of the current UTC day (midnight).
    /// </summary>
    public sealed class DateTimeMethodCall : MethodCall
    {
        /// <summary>
        /// Represents the <c>now()</c> RQL function. Returns the current UTC date and time on the server at query execution time.
        /// </summary>
        public static readonly DateTimeMethodCall Now = new DateTimeMethodCall(WhereToken.MethodsType.Now);

        /// <summary>
        /// Represents the <c>today()</c> RQL function. Returns the start of the current UTC day (midnight) on the server at query execution time.
        /// </summary>
        public static readonly DateTimeMethodCall Today = new DateTimeMethodCall(WhereToken.MethodsType.Today);

        public WhereToken.MethodsType MethodType { get; }

        private DateTimeMethodCall(WhereToken.MethodsType methodType)
        {
            MethodType = methodType;
            Args = Array.Empty<object>();
        }
    }
}
