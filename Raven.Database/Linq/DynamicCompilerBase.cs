using System;
using System.IO;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Util;

namespace Raven.Database.Linq
{
	public class DynamicCompilerBase
	{
		protected const string uniqueTextToken = "96E65595-1C9E-4BFB-A0E5-80BF2D6FC185";
		
		protected OrderedPartCollection<AbstractDynamicCompilationExtension> extensions;
		protected string basePath;
		protected string name;
		protected static readonly ConcurrentSet<string> paths = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		public string CompiledQueryText { get; set; }
		public Type GeneratedType { get; set; }
		public string Name
		{
			get { return name; }
		}
		public string CSharpSafeName { get; set; }

		public DynamicCompilerBase(InMemoryRavenConfiguration configuration, OrderedPartCollection<AbstractDynamicCompilationExtension> extensions, string name, string basePath)
		{
			this.name = name;
			this.extensions = extensions;
			if (configuration.RunInMemory == false)
			{
				this.basePath = Path.Combine(basePath, "TemporaryIndexDefinitionsAsSource");
				if (Directory.Exists(this.basePath) == false)
				{
					Directory.CreateDirectory(this.basePath);
				}
				else
				{
					MaybeCleanupDirectory(this.basePath);
				}
			}
			this.name = MonoHttpUtility.UrlEncode(name);
		}

		private static void MaybeCleanupDirectory(string path)
		{
			if (paths.TryAdd(path) == false)
				return;

			foreach (var file in Directory.GetFiles(path, "*.dll"))
			{
				try
				{
					File.Delete(file);
				}
				catch (Exception)
				{
					// failure here is expected, this is probably another index that is currently
					// live that is locking the file, we will get it next restart
				}
			}
		}

	}
}