using System.IO;
using Xunit;

namespace Raven.Scenarios
{
	public abstract class AbstractScenario
	{
		[Fact]
		public void Execute()
		{
			new Scenario(
				Path.Combine(AllScenariosWithoutExplicitScenario.ScenariosPath, GetType().Name) + ".saz"
				).Execute();
		}
	}

	public class PuttingDocumentUsingTransaction : AbstractScenario
	{
	}
}