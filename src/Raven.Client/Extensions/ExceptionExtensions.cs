using System;
using System.Text;

namespace Raven.Client.Extensions
{
    ///<summary>
    /// Extension methods to handle common scenarios
    ///</summary>
    internal static class ExceptionExtensions
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
                return ae.ExtractSingleInnerException();

            return e;
        }

        /// <remarks>Code from http://stackoverflow.com/questions/1886611/c-overriding-tostring-method-for-custom-exceptions </remarks>
        public static string ExceptionToString(
            this Exception ex,
            Action<StringBuilder> customFieldsFormatterAction)
        {
            var description = new StringBuilder();
            description.AppendFormat("{0}: {1}", ex.GetType().Name, ex.Message);

            customFieldsFormatterAction?.Invoke(description);

            if (ex.InnerException != null)
            {
                description.AppendFormat(" ---> {0}", ex.InnerException);
                description.AppendFormat(
                    "{0}   --- End of inner exception stack trace ---{0}",
                    Environment.NewLine);
            }

            description.Append(ex.StackTrace);

            return description.ToString();
        }

        /// <remarks>Code from http://stackoverflow.com/questions/1886611/c-overriding-tostring-method-for-custom-exceptions </remarks>
        public static string AllInnerMessages(this Exception ex)
        {
            var messages = new StringBuilder();
            messages.Append(ex.Message);

            var inner = ex.InnerException;
            while (inner != null)
            {
                messages.Append(" -> ");
                messages.Append(inner.Message);
                inner = inner.InnerException;
            }

            return messages.ToString();
        }
    }
}
