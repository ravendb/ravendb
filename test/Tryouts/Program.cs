using System;
using System.Diagnostics;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var sp = Stopwatch.StartNew();
            var jcp = new JsonContextPool();

            for (int i = 0; i < 1000 * 1000; i++)
            {
                JsonOperationContext context;
                using (jcp.AllocateOperationContext(out context))
                {
                    context.ReadObject(new DynamicJsonValue
                    {
                        ["Test"] = i,
                        ["Name"] = "frank",
                        ["Age"] = 91
                    }, "test");
                }
            }

            Console.WriteLine(sp.ElapsedMilliseconds.ToString("#,#"));
        }
    }
}