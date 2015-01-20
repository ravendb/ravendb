// -----------------------------------------------------------------------
//  <copyright file="AbstractFilePutTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;

using Raven.Database.FileSystem.Storage;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Plugins
{
	[InheritedExport]
	public abstract class AbstractFilePutTrigger : IRequiresFileSystemInitialization
	{
		public RavenFileSystem FileSystem { get; set; }

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

		public virtual VetoResult AllowPut(string name, RavenJObject headers, IStorageActionsAccessor accessor)
		{
			return VetoResult.Allowed;
		}

		public virtual void OnPut(string name, RavenJObject headers, IStorageActionsAccessor accessor)
		{
		}

		public virtual void AfterPut(string name, long? size, RavenJObject headers, IStorageActionsAccessor accessor)
		{
		}

		public virtual void OnUpload(string name, RavenJObject headers, int pageId, int pagePositionInFile, int pageSize, IStorageActionsAccessor accessor)
		{
		}

		public virtual void AfterUpload(string name, RavenJObject headers, IStorageActionsAccessor accessor)
		{
		}
	}
}