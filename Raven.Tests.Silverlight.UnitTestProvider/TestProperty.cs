namespace Raven.Tests.Silverlight.UnitTestProvider
{
	using Microsoft.Silverlight.Testing.UnitTesting.Metadata;

	public class TestProperty : ITestProperty
	{
		public TestProperty()
		{
		}

		public TestProperty(string name, string value)
		{
			Name = name;
			Value = value;
		}

		public string Name { get; set; }
		public string Value { get; set; }
	}
}