namespace System.IO
{
	using System;

	public class InvalidDataException : Exception
	{
		public InvalidDataException() { }
		public InvalidDataException(string message) : base(message) { }
		public InvalidDataException(string message, Exception innerException) : base(message, innerException) { }
	}
}