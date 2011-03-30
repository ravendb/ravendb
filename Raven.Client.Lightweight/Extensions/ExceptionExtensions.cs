#if !NET_3_5
namespace Raven.Client.Extensions
{
	using System;
	using System.Linq;

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
			var parts = e.Message.Split(new[] { "Exception:", "\r\n   " }, StringSplitOptions.None);
			return parts.First(x => !x.Contains("Exception:"));
		}
	}
}
#endif