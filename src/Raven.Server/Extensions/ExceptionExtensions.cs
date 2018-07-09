using System;

namespace Raven.Server.Extensions
{
    ///<summary>
    /// Extension methods to handle common scenarios
    ///</summary>
    public static class ExceptionExtensions
    {
            /// <summary>
            /// Recursively examines the inner exceptions of an <see cref="AggregateException"/> and returns a single child exception.
            /// </summary>
            /// <returns>
            /// If any of the aggregated exceptions have more than one inner exception, null is returned.
            /// </returns>
            public static Exception ExtractSingleInnerException(this AggregateException e)
        {
            if (e == null)
                return null;
            while (true)
            {
                if (e.InnerExceptions.Count != 1)
                    return e;

                var aggregateException = e.InnerExceptions[0] as AggregateException;
                if (aggregateException == null)
                    break;
                e = aggregateException;
            }

            return e.InnerExceptions[0];
        }

        public static Exception ExtractSingleInnerException(this Exception e)
        {
            if (e is AggregateException ae)
            {
                return ae.ExtractSingleInnerException();
            }

            return e;
        }
    }
}
