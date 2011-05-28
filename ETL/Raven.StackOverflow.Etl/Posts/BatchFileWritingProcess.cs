using System;
using System.IO;
using Rhino.Etl.Core.Operations;

namespace Raven.StackOverflow.Etl.Posts
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

			return result;
		}

	}
}