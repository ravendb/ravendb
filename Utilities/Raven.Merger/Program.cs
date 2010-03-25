using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ILMerging;

namespace Raven.Merger
{
	class Program
	{
		static int Main(string[] args)
		{
			try
			{
				var merge = new ILMerge
				{
					OutputFile = "RavenDb.exe",
					TargetKind = ILMerge.Kind.SameAsPrimaryAssembly,
					Version = new Version(4, 0)
				};
				merge.SetInputAssemblies(
					new[]
						{
							@"Raven.Server.exe",
							@"Raven.Database.dll",
							@"Esent.Interop.dll",
                            @"ICSharpCode.NRefactory.dll",
							@"Lucene.Net.dll",
							@"log4net.dll",
							@"Newtonsoft.Json.dll",

						});
				merge.SetTargetPlatform("4", Path.GetDirectoryName(typeof(object).Assembly.Location));
				merge.Merge();
				return 0;
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				return -1;
			}
		}
	}
}
