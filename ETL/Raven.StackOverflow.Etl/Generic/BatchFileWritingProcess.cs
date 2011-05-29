using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Raven.Abstractions.Commands;
using Rhino.Etl.Core.Operations;

namespace Raven.StackOverflow.Etl.Generic
{
	public abstract class BatchFileWritingProcess : AbstractOperation
	{
		protected BatchFileWritingProcess(string outputDirectory)
		{
			OutputDirectory = outputDirectory;
		}

		public string OutputDirectory { get; private set; }

		public string GetOutputPath(params string[] pathElements)
		{
			string result = OutputDirectory;

			foreach (var element in pathElements)
				result = Path.Combine(result, element);

			var parent = new DirectoryInfo(result).Parent;

			if (!parent.Exists)
				parent.Create();

			return result;
		}

		public void WriteCommandsTo(IEnumerable<ICommandData> commands, params string[] pathElements)
		{
			File.WriteAllText(GetOutputPath(pathElements),
				"[" + String.Join(",\n", commands.Select(c => c.ToJson().ToString())) + "]");
		}
	}
}