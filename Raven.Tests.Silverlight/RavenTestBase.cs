using System.Diagnostics;
using System.Linq;
using Raven.Abstractions;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Silverlight.UnitTestProvider;

namespace Raven.Tests.Silverlight
{
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
				.Replace("Raven.Tests.Silverlight.", string.Empty) + SystemTime.UtcNow.Ticks;
		}

		protected IDocumentStore NewDocumentStore()
		{
			return new DocumentStore { Url = Url + Port }.Initialize();
		}
	}
}