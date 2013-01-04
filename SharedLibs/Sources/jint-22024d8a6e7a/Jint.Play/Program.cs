using System;
using System.Diagnostics;
using System.IO;
using Jint.Native;

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
	        jint.SetMaxRecursions(50);
	        jint.SetMaxSteps(10*1000);

			sw.Start();
			try
			{
				Console.WriteLine(
					jint.Run(File.ReadAllText(@"C:\Work\ravendb-1.2\SharedLibs\Sources\jint-22024d8a6e7a\Jint.Play\test.js")));
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
	        finally 
	        {
				Console.WriteLine("{0}ms", sw.ElapsedMilliseconds);
			}

        }
    }
}

