using System.IO;
using Xunit;
using System.Linq;

namespace Raven.Scenarios
{
    public abstract class AbstractScenario
    {
        [Fact]
        public void Execute()
        {
            new Scenario(
                Path.Combine(AllScenariosWithoutExplicitScenario.ScenariosPath,GetType().Namespace.Split('.').Last())
                ).Execute();
        }
    }
}