// -----------------------------------------------------------------------
//  <copyright file="AbstractFileSystemIndexCodec.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Database.Plugins;

namespace Raven.Database.FileSystem.Plugins
{
	[InheritedExport]
	public abstract class AbstractFileSystemIndexCodec : AbstractBaseIndexCodec, IRequiresFileSystemInitialization
	{
		public virtual void Initialize(RavenFileSystem fileSystem)
		{
		}

		public virtual void SecondStageInit()
		{
		}
	}
}