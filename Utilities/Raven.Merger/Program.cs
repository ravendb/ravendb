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
				Merge("RavenDb.exe", new[]
				{
					@"Raven.Server.exe",
					@"Raven.Database.dll",
					@"Esent.Interop.dll",
					@"ICSharpCode.NRefactory.dll",
					@"Lucene.Net.dll",
					@"log4net.dll",
					@"Newtonsoft.Json.dll",
					@"Rhino.Licensing.dll",
				});
				
				Merge("RavenWeb.dll", new[]
				{
					@"Rhino.Licensing.dll",
					@"Raven.Web.dll",
					@"Raven.Database.dll",
					@"Esent.Interop.dll",
					@"ICSharpCode.NRefactory.dll",
					@"Lucene.Net.dll",
					@"log4net.dll",
					@"Newtonsoft.Json.dll",
				});

				//Merge("RavenClient.dll", new[]
				//{
				//    @"Raven.Client.dll",
				//    @"Raven.Database.dll",
				//    @"Esent.Interop.dll",
				//    @"ICSharpCode.NRefactory.dll",
				//    @"Lucene.Net.dll",
				//    @"log4net.dll",
				//    @"Newtonsoft.Json.dll",
				//});
				return 0;
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				return -1;
			}
		}

	  private static void Merge(string outputFile, string[] inputAssemblies)
		{
			var merge = new ILMerge
			{
				OutputFile = outputFile,
				TargetKind = ILMerge.Kind.SameAsPrimaryAssembly,
				Version = new Version(4, 0),
			};
			merge.SetInputAssemblies(inputAssemblies);
			merge.SetTargetPlatform("4", Path.GetDirectoryName(typeof(object).Assembly.Location));
			merge.Merge();
		}
	}
}
