using Xunit;
using System.Linq;

namespace Rhino.DibanDB.Scenarios
{
    public abstract class AbstractScenario
    {
        public string Directory
        {
            get { return @"C:\Work\rhino-divandb\Rhino.DibanDB.Scenarios\" + GetType().Namespace.Split('.').Last(); }
        }

        [Fact]
        public void Execute()
        {
           new Scenario(Directory).Execute();
        }
    }
}