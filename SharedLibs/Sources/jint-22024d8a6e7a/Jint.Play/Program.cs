using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Reflection;
using System.Diagnostics;
using Jint.Native;
using Jint.Debugger;

namespace Jint.Play
{
    class Program
    {
        static void Main(string[] args)
        {

            var assembly = Assembly.Load("Jint.Tests");
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

            try
            {
                Console.WriteLine(jint.Run("2+3"));
                Console.WriteLine("after0");
                Console.WriteLine(jint.Run(")(---"));
                Console.WriteLine("after1");
                Console.WriteLine(jint.Run("FOOBAR"));
                Console.WriteLine("after2");

            }
            catch (JintException e)
            {
                Console.WriteLine(e.Message);
            }

            Console.WriteLine("{0}ms", sw.ElapsedMilliseconds);
        }
    }
}

