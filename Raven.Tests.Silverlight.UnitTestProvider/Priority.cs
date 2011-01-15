namespace Raven.Tests.Silverlight.UnitTestProvider
{
	using System.Globalization;
	using Microsoft.Silverlight.Testing.UnitTesting.Metadata;

	public class Priority : IPriority
	{
		public Priority(int priority)
		{
			Value = priority;
		}

		public int Value { get; private set; }


		public override string ToString()
		{
			return Value.ToString(CultureInfo.InvariantCulture);
		}
	}
}