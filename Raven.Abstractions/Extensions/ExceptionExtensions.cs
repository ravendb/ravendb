using System;
using System.IO;
using System.Linq;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;

#if !NET35
namespace Raven.Abstractions.Extensions
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
					return null;

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

        public static string TryReadResponseIfWebException(this Exception ex)
        {
            if (!(ex is WebException))
            {
                return string.Empty;
            }

            var webException = ex as WebException;

            if (webException.Response == null)
            {
                return string.Empty;
            }

            using (var reader = new StreamReader(webException.Response.GetResponseStream()))
            {
                var response = reader.ReadToEnd();
                return response;
            }
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
	}
}
#endif
