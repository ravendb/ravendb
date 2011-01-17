namespace Raven.Tests.Silverlight
{
	using System;
	using UnitTestProvider;

	public abstract class RavenTestBase : AsynchronousTaskTest
	{
		protected const int Port = 8080;
		protected const string Url = "http://localhost:";

		protected static string GenerateNewDatabaseName()
		{
			return Guid.NewGuid().ToString();
		}
	}
}