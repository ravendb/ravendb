using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Tryouts.Json
{
    public static class PerfTest
    {
        private const string BigJsonPath = @"z:\Pool5.json";
        private const int IterationsCount = 100;

        public static void RunPerfTest()
        {
            var stopWatch = new Stopwatch();
            Console.WriteLine("Starting Json performance test with " + IterationsCount + " iterations...");

            Console.WriteLine("Using Newtonsoft Json.NET containers:");
            stopWatch.Start();
            var jo = ExecuteManyFileReads(BigJsonPath);
            stopWatch.Stop();
            Console.WriteLine("Reading took " + stopWatch.ElapsedMilliseconds + " ms");

            stopWatch.Reset();
            stopWatch.Start();
            CloneALot(jo);
            stopWatch.Stop();
            Console.WriteLine("Cloning took " + stopWatch.ElapsedMilliseconds + " ms");

            Console.WriteLine("Using Raven.Json containers:");
            stopWatch.Reset();
            stopWatch.Start();
            var jo2 = ExecuteManyFileReads2(BigJsonPath);
            stopWatch.Stop();
            Console.WriteLine("Reading took " + stopWatch.ElapsedMilliseconds + " ms");

            stopWatch.Reset();
            stopWatch.Start();
            CloneALot2(jo2);
            stopWatch.Stop();
            Console.WriteLine("Cloning took " + stopWatch.ElapsedMilliseconds + " ms");
        }

        public static JObject ExecuteManyFileReads(string filePath)
        {
            string text = File.ReadAllText(filePath);
            JObject temp = null;
            for (int i = 0; i < IterationsCount; i++)
            {
                using (var streamReader = new StringReader(text))
                using (var jsonReader = new JsonTextReader(streamReader))
                    temp = JObject.Load(jsonReader);
            }
            return temp;
        }

        public static RavenJObject ExecuteManyFileReads2(string filePath)
        {
            string text = File.ReadAllText(filePath);
            RavenJObject temp = null;
            for (int i = 0; i < IterationsCount; i++)
            {
                using (var streamReader = new StringReader(text))
                using (var jsonReader = new JsonTextReader(streamReader))
                    temp = RavenJObject.Load(jsonReader);
            }
            return temp;
        }

        public static void CloneALot(JToken jt)
        {
            JToken temp;
            for (int i = 0; i < IterationsCount; i++)
                temp = jt.DeepClone();
        }

        public static void CloneALot2(RavenJToken jt)
        {
            RavenJToken temp;
            for (int i = 0; i < IterationsCount; i++)
                temp = jt.CloneToken();
        }

        public static JToken CloneJsonObject(JToken jo)
        {
            return jo.DeepClone();
        }
    }
}
