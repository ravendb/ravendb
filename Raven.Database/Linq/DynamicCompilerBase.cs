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
		protected readonly InMemoryRavenConfiguration configuration;
		protected string name;
		private string cSharpSafeName;
		public string CompiledQueryText { get; set; }
		public Type GeneratedType { get; set; }
		public string Name
		{
          get { return name; }
		}
		public string CSharpSafeName
		{
			get { return cSharpSafeName; }
			set
			{
				cSharpSafeName = value;
				if (cSharpSafeName == null)
					return;
				if (cSharpSafeName.Length > 256)
				{
					cSharpSafeName = cSharpSafeName.Substring(0, 256);
				}
			}
		}

		public DynamicCompilerBase(InMemoryRavenConfiguration configuration, OrderedPartCollection<AbstractDynamicCompilationExtension> extensions, string name, string basePath)
		{
			this.configuration = configuration;
			this.name = name;
			this.extensions = extensions;
			if (configuration.RunInMemory == false)
			{
				this.basePath = Path.Combine(basePath, "temp");
				if (Directory.Exists(this.basePath) == false)
					Directory.CreateDirectory(this.basePath);
			}
		}
	}
}