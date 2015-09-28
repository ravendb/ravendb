// -----------------------------------------------------------------------
//  <copyright file="BaseCountersApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Common;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Counters.Controllers
{
	public abstract class BaseCountersApiController : ResourceApiController<CounterStorage, CountersLandlord>
	{
		protected string CountersName
		{
			get
			{
				return ResourceName;
			}
		}

		protected CounterStorage CounterStorage
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
				return ResourceType.Counter;
			}
		}

		public override void MarkRequestDuration(long duration)
		{
			if (Resource == null)
				return;
			Resource.MetricsCounters.RequestDurationMetric.Update(duration);
		}
	}
}