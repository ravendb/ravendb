namespace System.IO
{
	public class InvalidDataException : Exception
	{
		public InvalidDataException()
		{
		}

		public InvalidDataException(string message) : base(message)
		{
		}

		public InvalidDataException(string message, Exception inner) : base(message, inner)
		{
		}
	}
}