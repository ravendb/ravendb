using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Client.Connection;
using Raven.Client.Data;
using System.Reflection;
using Raven.Abstractions.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
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
                .UsingFormatter(new TsFormatter
                {
                    EnumsAsString = true
                });

            scripter
               .WithTypeMapping(TsPrimitive.String, typeof(Guid))
               .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(HashSet<>))
               .WithTypeMapping(TsPrimitive.Any, typeof(RavenJObject))
               .WithTypeMapping(TsPrimitive.Any, typeof(RavenJValue))
               .WithTypeMapping(TsPrimitive.String, typeof(DateTime))
               .WithTypeMapping(new TsArray(TsPrimitive.Any, 1), typeof(RavenJArray))
               .WithTypeMapping(TsPrimitive.Any, typeof(RavenJToken));

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
            };


            scripter.UsingTypeFilter(type => ignoredTypes.Contains(type) == false);
            scripter.UsingTypeReader(new TypeReaderWithIgnoreMethods());

            scripter.AddType(typeof(DatabaseStatistics));
            scripter.AddType(typeof(IndexDefinition));

            // notifications
            scripter.AddType(typeof(OperationStatusChangeNotification));
            scripter.AddType(typeof(DeterminateProgress));
            scripter.AddType(typeof(IndeterminateProgress));
            scripter.AddType(typeof(DocumentChangeNotification));
            scripter.AddType(typeof(IndexChangeNotification));
            scripter.AddType(typeof(TransformerChangeNotification));
            scripter.AddType(typeof(DatabaseOperations.PendingOperation));


            return scripter;
        }
    }
}
