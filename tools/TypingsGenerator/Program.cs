using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Client.Connection;
using Raven.Client.Data;
using System.Reflection;
using Raven.Client.Data.Queries;
using Raven.Json.Linq;
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
            Directory.Delete(TargetDirectory, true);
            Directory.CreateDirectory(TargetDirectory);
            scripter
                .SaveToDirectory(TargetDirectory);
        }

        private static Scripter ConfigureTypes(Scripter scripter)
        {
            var ignoredTypes = new HashSet<Type>
            {
                typeof(RavenJObject),
                typeof(RavenJValue),
                typeof(RavenJArray),
                typeof(RavenJToken)
            };


            scripter.UsingTypeFilter(type => ignoredTypes.Contains(type) == false);

            scripter.AddType(typeof(DatabaseStatistics));
            scripter.AddType(typeof(QueryResult));

            return scripter;
        }
    }
}
