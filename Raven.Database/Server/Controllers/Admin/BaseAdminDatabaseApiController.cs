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
		public override ResourceType ResourceType {
			get { return ResourceType.Database; }
		}

		public string DatabaseName
		{
			get { return ResourceName; }
		}

		public DocumentDatabase Database
		{
			get { return Resource; }
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