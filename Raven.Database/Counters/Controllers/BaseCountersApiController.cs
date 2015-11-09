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
        protected string CountersName => ResourceName;

	    protected CounterStorage CounterStorage => Resource;

	    public override ResourceType ResourceType => ResourceType.Counter;

	    public override void MarkRequestDuration(long duration)
        {
		    Resource?.MetricsCounters.RequestDurationMetric.Update(duration);
        }
    }
}
