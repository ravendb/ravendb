// -----------------------------------------------------------------------
//  <copyright file="BaseAdminDatabaseApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Common;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Server.Controllers.Admin
{
	public class BaseAdminDatabaseApiController : AdminResourceApiController<DocumentDatabase, DatabasesLandlord>
	{
		public virtual string DatabaseName
		{
			get
			{
				return ResourceName;
			}
		}

		public virtual DocumentDatabase Database
		{
			get
			{
				return Resource;
			}
		}

		public override ResourceType ResourceType
		{
			get
			{
				return ResourceType.Database;
			}
		}

		public override void MarkRequestDuration(long duration)
		{
			if (Resource == null)
				return;
			Resource.WorkContext.MetricsCounters.RequestDurationMetric.Update(duration);
			Resource.WorkContext.MetricsCounters.RequestDurationLastMinute.AddRecord(duration);
		}
	}
}