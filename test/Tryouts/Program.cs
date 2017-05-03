using System;
using System.Collections.Generic;
using System.Diagnostics;
using FastTests.Client.Attachments;
using FastTests.Smuggler;
using System.Threading.Tasks;
using FastTests.Issues;
using FastTests.Server.Documents.Indexing;
using FastTests.Server.Documents.PeriodicExport;
using FastTests.Server.OAuth;
using FastTests.Server.Replication;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using SlowTests.Issues;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Tryouts
{
    public class Program
    {
        public class Obj3
        {
            public string Name;
        }
        public class Obj2
        {
            public Obj3 Obj;
        }
        public class ClassWithLongDictionary
        {
            public Dictionary<long, Obj2> Dic;
        }
        public class Desirializer2 : JsonDeserializationBase
        {
            public Func<BlittableJsonReaderObject, ClassWithLongDictionary> Dic = GenerateJsonDeserializationRoutine<ClassWithLongDictionary>();
        }
        
        public static void Main(string[] args)
        {
            var @class = new ClassWithLongDictionary()
            {
                Dic = new Dictionary<long, Obj2>()
                {
                    [1]= new Obj2(){Obj = new Obj3(){Name="2"}},
                    [2] = new Obj2() { Obj = new Obj3() { Name = "3" } },
                    [3] = new Obj2() { Obj = new Obj3() { Name = "4" } },
                    
                }
            };


            var deserializer = new Desirializer2();
            using (var context = new JsonOperationContext(1024, 1024))
            {
                var reader = EntityToBlittable.ConvertEntityToBlittable(@class, new DocumentConventions(), context);
                
                var obj = deserializer.Dic(reader);
                foreach (var l in obj.Dic)
                {
                    Console.WriteLine($"{l.Key}:{l.Value}");
                }
            }
        }
    }
}