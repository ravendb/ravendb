using System.Collections.Generic;
using System.IO;
using Xunit;
using Xunit.Extensions;

namespace Rhino.DibanDB.Scenarios
{
    public class AllScenariosWithoutExplicitScenario
    {
        [Theory]
        [PropertyData("ScenariosWithoutExplicitScenario")]
        public void Execute(string directory)
        {
            new Scenario(Path.Combine(ScenariosPath, directory)).Execute();
        }

        public static string ScenariosPath
        {
            get
            {
                return Directory.Exists(@"..\..\bin") // running in VS
                    ? @"..\..\" : @"..\Rhino.DibanDB.Scenarios";
            }
        }

        public static IEnumerable<object[]> ScenariosWithoutExplicitScenario
        {
            get
            {
                foreach (var directory in Directory.GetDirectories(ScenariosPath))
                {
                    var dir = Path.GetFileName(directory);
                    if(dir.Equals("bin") || dir.Equals("obj") || dir.Equals("Properties"))
                        continue;
                    if (typeof(Scenario).Assembly.GetType("Rhino.DibanDB.Scenarios." + dir +".Scenario") != null)
                        continue;
                    yield return new object[] {dir};
                };
            }
        }
    }
}