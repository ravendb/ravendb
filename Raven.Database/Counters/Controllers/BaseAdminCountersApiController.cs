// -----------------------------------------------------------------------
//  <copyright file="BaseCountersApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Database.Common;
using Raven.Database.Config;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Counters.Controllers
{
	public abstract class BaseAdminCountersApiController : AdminResourceApiController<CounterStorage, CountersLandlord>
	{
		public override InMemoryRavenConfiguration ResourceConfiguration
		{
			get
			{
				throw new NotSupportedException("Use Counters.Configuration instead.");
			}
		}

		public string CounterName => ResourceName;

		public CounterStorage Counters => Resource;

		public override ResourceType ResourceType => ResourceType.Counter;

		public override void MarkRequestDuration(long duration)
		{
			if (Counters == null)
				return;
			Counters.MetricsCounters.RequestDurationMetric.Update(duration);
		}
	}
}