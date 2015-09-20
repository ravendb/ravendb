// -----------------------------------------------------------------------
//  <copyright file="BaseAdminFileSystemApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Database.Common;
using Raven.Database.Config;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.FileSystem.Controllers
{
	public class BaseAdminFileSystemApiController : AdminResourceApiController<RavenFileSystem, FileSystemsLandlord>
	{
		public override InMemoryRavenConfiguration ResourceConfiguration
		{
			get
			{
				throw new NotSupportedException("Use FileSystem.Configuration instead.");
			}
		}

		public string FileSystemName => ResourceName;

		public RavenFileSystem FileSystem => Resource;

		public override ResourceType ResourceType => ResourceType.FileSystem;

		public override void MarkRequestDuration(long duration)
		{
			if (FileSystem == null)
				return;
			FileSystem.MetricsCounters.RequestDurationMetric.Update(duration);
			FileSystem.MetricsCounters.RequestDurationLastMinute.AddRecord(duration);
		}
	}
}