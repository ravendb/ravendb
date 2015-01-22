// -----------------------------------------------------------------------
//  <copyright file="AbstractFilePutTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;

using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Plugins
{
	[InheritedExport]
	public abstract class AbstractFilePutTrigger : IRequiresFileSystemInitialization
	{
		public RavenFileSystem FileSystem { get; private set; }

		public virtual void Initialize(RavenFileSystem fileSystem)
		{
			FileSystem = fileSystem;
			Initialize();
		}

		public virtual void Initialize()
		{
		}

		public virtual void SecondStageInit()
		{
		}

		public virtual VetoResult AllowPut(string name, RavenJObject headers)
		{
			return VetoResult.Allowed;
		}

		public virtual void OnPut(string name, RavenJObject headers)
		{
		}

		public virtual void AfterPut(string name, long? size, RavenJObject headers)
		{
		}

		public virtual void OnUpload(string name, RavenJObject headers, int pageId, int pagePositionInFile, int pageSize)
		{
		}

		public virtual void AfterUpload(string name, RavenJObject headers)
		{
		}
	}
}