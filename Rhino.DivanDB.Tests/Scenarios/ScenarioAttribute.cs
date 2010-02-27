using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Xunit;
using Xunit.Sdk;
using Enumerable = System.Linq.Enumerable;

namespace Rhino.DivanDB.Tests.Scenarios
{
    public class ScenarioAttribute : FactAttribute
    {
        public string Path { get; set; }

        protected override IEnumerable<ITestCommand> EnumerateTestCommands(MethodInfo method)
        {
            return Enumerable.Select(Directory.GetDirectories(Path), directory => new ScenarioCommand(directory, method) as ITestCommand);
        }
    }
}