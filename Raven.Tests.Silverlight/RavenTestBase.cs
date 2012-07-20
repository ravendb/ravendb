using System.Diagnostics;
using System.Linq;
using Microsoft.Silverlight.Testing;
using Raven.Abstractions;
namespace Raven.Tests.Silverlight
{
	using System;
	using UnitTestProvider;

	public abstract class RavenTestBase : AsynchronousTaskTest
	{
		protected const int Port = 8079;
		protected const string Url = "http://localhost:";

		protected static string GenerateNewDatabaseName()
		{
			var stackTrace = new StackTrace();
			var stackFrame =
				stackTrace.GetFrames().First(x => 
					x.GetMethod().Name == "MoveNext" && 
					x.GetMethod().DeclaringType.FullName.Contains("+<"));
			
			var generateNewDatabaseName = stackFrame.GetMethod().DeclaringType.FullName.Replace("+<",".");

			return generateNewDatabaseName
				.Substring(0, generateNewDatabaseName.IndexOf(">"))
				.Replace("Raven.Tests.Silverlight.",string.Empty) + SystemTime.Now.Ticks;
		}
	}
}