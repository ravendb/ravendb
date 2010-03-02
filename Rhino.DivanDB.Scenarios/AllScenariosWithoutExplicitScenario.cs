using System.Collections.Generic;
using System.IO;
using Xunit.Extensions;

namespace Rhino.DivanDB.Scenarios
{
    public class AllScenariosWithoutExplicitScenario
    {
        [Theory]
        [PropertyData("ScenariosWithoutExplicitScenario")]
        public void Execute(string file)
        {
            new Scenario(file).Execute();
        }

        public static string ScenariosPath
        {
            get
            {
                return Directory.Exists(@"..\..\bin") // running in VS
                           ? @"..\..\Scenarios" : @"..\Rhino.DivanDB.Scenarios\Scenarios";
            }
        }

        public static IEnumerable<object[]> ScenariosWithoutExplicitScenario
        {
            get
            {
                foreach (var file in Directory.GetFiles(ScenariosPath,"*.saz"))
                {
                    if (typeof(Scenario).Assembly.GetType("Rhino.DivanDB.Scenarios." + Path.GetFileNameWithoutExtension(file) +"Scenario") != null)
                        continue;
                    yield return new object[] {file};
                };
            }
        }
    }
}