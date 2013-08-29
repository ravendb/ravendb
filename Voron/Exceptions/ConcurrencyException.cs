namespace Voron.Exceptions
{
	using System;

	public class ConcurrencyException : Exception
	{
		public ConcurrencyException(string message)
			: base(message)
		{
		}
	}
}