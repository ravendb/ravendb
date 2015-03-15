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
	public abstract class AbstractFileRenameTrigger : IRequiresFileSystemInitialization
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

		public virtual VetoResult AllowRename(string name, string newName)
		{
			return VetoResult.Allowed;
		}

		public virtual void OnRename(string name, RavenJObject metadata)
		{
		}

		public virtual void AfterRename(string name, string newName, RavenJObject metadata)
		{
		}
	}
}