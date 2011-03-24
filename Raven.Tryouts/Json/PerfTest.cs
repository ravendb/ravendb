using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Raven.Tryouts.Json
{
    public static class PerfTest
    {
        private const string BigJsonPath = @"z:\Pool5.json";

        public static void RunPerfTest()
        {
            var stopWatch = new Stopwatch();
            Console.WriteLine("Starting Json performence test...");
            
            stopWatch.Start();
            var jo = ExecuteManyFileReads(BigJsonPath);
            stopWatch.Stop();
            Console.WriteLine("Reading took " + stopWatch.ElapsedMilliseconds + " ms");

            stopWatch.Reset();
            stopWatch.Start();
            CloneALot(jo);
            stopWatch.Stop();
            Console.WriteLine("Cloning took " + stopWatch.ElapsedMilliseconds + " ms");
        }

        public static JObject ExecuteManyFileReads(string filePath)
        {
                JObject temp = null;
                for (int i = 0; i < 100; i++)
                {
                    using (var streamReader = File.OpenText(filePath))
                    using (var jsonReader = new JsonTextReader(streamReader))
                        temp = JObject.Load(jsonReader);
                }
                return temp;
        }

        public static void CloneALot(JToken jt)
        {
            JToken temp;
            for (int i = 0; i < 100; i++)
                temp = jt.DeepClone();
        }

        public static JToken CloneJsonObject(JToken jo)
        {
            return jo.DeepClone();
        }
    }
}
