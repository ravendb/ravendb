using System;
using System.Diagnostics;

namespace Jint.Play
{
    class Program
    {
        static void Main(string[] args)
        {

            Stopwatch sw = new Stopwatch();

            // string script = new StreamReader(assembly.GetManifestResourceStream("Jint.Tests.Parse.coffeescript-debug.js")).ReadToEnd();
            JintEngine jint = new JintEngine()
                // .SetDebugMode(true)
                .DisableSecurity()
                .SetFunction("print", new Action<string>(Console.WriteLine))
                .SetFunction("write", new Action<string>(t => Console.WriteLine(t)))
                .SetFunction("stop", new Action(delegate() { Console.WriteLine(); }));
            sw.Reset();
            sw.Start();

            Console.WriteLine(jint.Run("Math.floor(1.5)"));
           

            Console.WriteLine("{0}ms", sw.ElapsedMilliseconds);
        }
    }
}

