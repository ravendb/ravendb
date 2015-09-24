// -----------------------------------------------------------------------
//  <copyright file="BaseAdminFileSystemApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Common;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.FileSystem.Controllers
{
	public class BaseAdminFileSystemApiController : AdminResourceApiController<RavenFileSystem, FileSystemsLandlord>
	{
		public string FileSystemName
		{
			get { return ResourceName; }
		}

		public RavenFileSystem FileSystem
		{
			get { return Resource; }
		}

		public override ResourceType ResourceType
		{
			get
			{
				return ResourceType.FileSystem;
			}
		}

		public override void MarkRequestDuration(long duration)
		{
			if (FileSystem == null)
				return;
			FileSystem.MetricsCounters.RequestDurationMetric.Update(duration);
			FileSystem.MetricsCounters.RequestDurationLastMinute.AddRecord(duration);
		}
	}
}