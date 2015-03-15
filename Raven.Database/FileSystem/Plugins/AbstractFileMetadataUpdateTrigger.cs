// -----------------------------------------------------------------------
//  <copyright file="AbstractFileMetadataUpdateTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Plugins
{
	[InheritedExport]
	public abstract class AbstractFileMetadataUpdateTrigger : IRequiresFileSystemInitialization
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

		public virtual VetoResult AllowUpdate(string name, RavenJObject metadata)
		{
			return VetoResult.Allowed;
		}

		public virtual void OnUpdate(string name, RavenJObject metadata)
		{
		}

		public virtual void AfterUpdate(string name, RavenJObject metadata)
		{
		}
	}
}