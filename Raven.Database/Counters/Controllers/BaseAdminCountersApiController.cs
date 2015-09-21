// -----------------------------------------------------------------------
//  <copyright file="BaseCountersApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Common;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Counters.Controllers
{
	public abstract class BaseAdminCountersApiController : AdminResourceApiController<CounterStorage, CountersLandlord>
	{
		public override ResourceType ResourceType
		{
			get
			{
				return ResourceType.Counter;
			}
		}

		public string CounterName
		{
			get { return ResourceName; }
		}

		public CounterStorage Counters
		{
			get { return Resource; }
		}

		public override void MarkRequestDuration(long duration)
		{
			if (Counters == null)
				return;
			Counters.MetricsCounters.RequestDurationMetric.Update(duration);
		}
	}
}