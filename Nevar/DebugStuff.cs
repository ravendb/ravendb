using System;
using System.Diagnostics;
using System.IO;

namespace Nevar
{
	public class DebugStuff
	{
		[Conditional("DEBUG")]
		public static void RenderAndShow(Transaction tx, Page start)
		{
			if (Debugger.IsAttached == false)
				return;
			var path = Path.Combine(Environment.CurrentDirectory, "output.dot");
			TreeDumper.Dump(tx, path, start);

			var output = Path.Combine(Environment.CurrentDirectory, "output.png");
			var p = Process.Start(@"C:\Users\Ayende\Downloads\graphviz-2.30.1\graphviz\bin\dot.exe", "-Tpng  " + path + " -o " + output);
			p.WaitForExit();
			Process.Start(output);
		} 
	}
}