using Raven.Tests.MultiGet;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.StressTests.Races
{
	public class Tobi : StressTest
	{
		[Fact]
		public void LazyMultiLoadOperationWouldBeInTheSession_WithNonStaleResponse()
		{
			Run<MultiGetQueries>(x => x.LazyMultiLoadOperationWouldBeInTheSession_WithNonStaleResponse());
		}

		[Fact]
		public void GetDocumentAfterAnEtagWhileAddingDocsFromMultipleThreadsEnumeratesAllDocs()
		{
			Run<GeneralStorage>(x => x.GetDocumentAfterAnEtagWhileAddingDocsFromMultipleThreadsEnumeratesAllDocs(), 250);
		}
	}
}