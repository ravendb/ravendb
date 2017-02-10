using System;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Raven.Client.Extensions
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

        /// <summary>
        /// Extracts a portion of an exception for a user friendly display
        /// </summary>
        /// <param name="e">The exception.</param>
        /// <returns>The primary portion of the exception message.</returns>
        public static string SimplifyError(this Exception e)
        {
            var parts = e.Message.Split(new[] { "\r\n   " }, StringSplitOptions.None);
            var firstLine = parts.First();
            var index = firstLine.IndexOf(':');
            return index > 0
                ? firstLine.Remove(0, index + 2)
                : firstLine;
        }

        public static string TryReadErrorPropertyFromJson(this string errorString)
        {
            if (string.IsNullOrEmpty(errorString) || !errorString.StartsWith("{"))
            {
                return string.Empty;
            }

            try
            {
                var jObject = JObject.Parse(errorString);
                if (jObject["Error"] != null)
                {
                    return (string)jObject["Error"];
                }
                else
                {
                    return string.Empty;
                }
            }
            catch (JsonReaderException)
            {
                return string.Empty;
            }
        }

        /// <remarks>Code from http://stackoverflow.com/questions/1886611/c-overriding-tostring-method-for-custom-exceptions </remarks>
        public static string ExceptionToString(
            this Exception ex,
            Action<StringBuilder> customFieldsFormatterAction)
        {
            var description = new StringBuilder();
            description.AppendFormat("{0}: {1}", ex.GetType().Name, ex.Message);

            if (customFieldsFormatterAction != null)
                customFieldsFormatterAction(description);

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
    }
}
