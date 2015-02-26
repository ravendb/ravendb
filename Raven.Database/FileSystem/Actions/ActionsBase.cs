// -----------------------------------------------------------------------
//  <copyright file="ActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Logging;

namespace Raven.Database.FileSystem.Actions
{
	public abstract class ActionsBase
	{
		protected RavenFileSystem FileSystem { get; private set; }

		protected ILog Log { get; private set; }

		protected ActionsBase(RavenFileSystem fileSystem, ILog log)
		{
			FileSystem = fileSystem;
			Log = log;
		}
	}
}