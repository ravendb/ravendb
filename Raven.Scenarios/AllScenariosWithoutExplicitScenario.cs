using System.Collections.Generic;
using System.IO;
using Xunit.Extensions;

namespace Raven.Scenarios
{
    public class AllScenariosWithoutExplicitScenario
    {
        public static string ScenariosPath
        {
            get
            {
                return Directory.Exists(@"..\..\bin") // running in VS
                           ? @"..\..\Scenarios"
                           : @"..\Raven.Scenarios\Scenarios";
            }
        }

        public static IEnumerable<object[]> ScenariosWithoutExplicitScenario
        {
            get
            {
                foreach (var file in Directory.GetFiles(ScenariosPath, "*.saz"))
                {
                    if (
                        typeof (Scenario).Assembly.GetType("Raven.Scenarios." + Path.GetFileNameWithoutExtension(file) +
                                                           "Scenario") != null)
                        continue;
                    yield return new object[] {Path.GetFileNameWithoutExtension(file)};
                }
                ;
            }
        }

        [Theory]
        [PropertyData("ScenariosWithoutExplicitScenario")]
        public void Execute(string file)
        {
            new Scenario(Path.Combine(ScenariosPath, file + ".saz")).Execute();
        }
    }
}