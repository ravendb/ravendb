// -----------------------------------------------------------------------
//  <copyright file="AbstractFileSystemIndexCodec.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

namespace Raven.Database.FileSystem.Plugins
{
	public abstract class AbstractFileSystemIndexCodec : IRequiresFileSystemInitialization
	{
		public void Initialize(RavenFileSystem fileSystem)
		{
		}

		public void SecondStageInit()
		{
		}

		public abstract Stream Encode(string key, Stream dataStream);

		public abstract Stream Decode(string key, Stream dataStream);
	}
}