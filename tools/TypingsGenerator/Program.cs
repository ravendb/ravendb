using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Client.Connection;
using Raven.Client.Data;
using System.Reflection;
using Raven.Server.Documents;
using TypeScripter;
using TypeScripter.TypeScript;

namespace TypingsGenerator
{
    public class Program
    {

        public const string TargetDirectory = "../../src/Raven.Studio/typings/server/";
        public static void Main(string[] args)
        {
            Directory.CreateDirectory(TargetDirectory);


            var scripter = new Scripter()
                .UsingAssemblies(new[] { typeof(Default).GetTypeInfo().Assembly, typeof(DocumentDatabase).GetTypeInfo().Assembly })
                .UsingFormatter(new TsFormatter
                    {
                        EnumsAsString = true
                    });
            scripter = ConfigureTypes(scripter);
            scripter
                .SaveToDirectory(TargetDirectory);
        }

        private static Scripter ConfigureTypes(Scripter scripter)
        {
            scripter.AddType(typeof(DatabaseStatistics));
            return scripter;
        }
    }
}
