using System;
using System.Diagnostics;
using System.IO;

namespace Nevar
{
	public class DebugStuff
	{
		[Conditional("DEBUG")]
		public static void RenderAndShow(Transaction tx, Page start, int showNodesEvery = 25, string format = "svg")
		{
			if (Debugger.IsAttached == false)
				return;
			var path = Path.Combine(Environment.CurrentDirectory, "output.dot");
			TreeDumper.Dump(tx, path, start, showNodesEvery);

			var output = Path.Combine(Environment.CurrentDirectory, "output." + format);
			var p = Process.Start(@"C:\Users\Ayende\Downloads\graphviz-2.30.1\graphviz\bin\dot.exe", "-T" + format + " " + path + " -o " + output);
			p.WaitForExit();
			Process.Start(output);
		} 
	}
}