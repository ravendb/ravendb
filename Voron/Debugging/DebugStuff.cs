using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Voron.Impl;

namespace Voron.Debugging
{
	public class DebugStuff
	{
		[Conditional("DEBUG")]
		public static void RenderAndShow(Transaction tx, long startPageNumber, int showNodesEvery = 25, string format = "svg")
		{
			if (Debugger.IsAttached == false)
				return;
			var path = Path.Combine(Environment.CurrentDirectory, "output.dot");
			TreeDumper.Dump(tx, path, tx.GetReadOnlyPage(startPageNumber), showNodesEvery);

			var output = Path.Combine(Environment.CurrentDirectory, "output." + format);
			var p = Process.Start(@"C:\Program Files (x86)\Graphviz2.30\bin\dot.exe", "-T" + format + " " + path + " -o " + output);
			p.WaitForExit();
			Process.Start(output);
			Thread.Sleep(500);
		} 
	}
}