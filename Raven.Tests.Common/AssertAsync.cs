using System;
using System.Threading.Tasks;

using Xunit.Sdk;

namespace Raven.Tests.Common
{
	/// <summary>
	/// Utility class to be used until xUnit supports async method to be used as parameters.
	/// </summary>
	public static class AssertAsync
	{
		public static async Task<TException> Throws<TException>(Func<Task> func) where TException : Exception
		{
			Type actual = null;
			try
			{
				await func();
			}
			catch (Exception e)
			{
				if (typeof (TException) == e.GetType())
					return (TException) e;
				throw new ThrowsException(typeof (TException), e);
			}
			throw new ThrowsException(typeof(TException), null);
		}
	}
}