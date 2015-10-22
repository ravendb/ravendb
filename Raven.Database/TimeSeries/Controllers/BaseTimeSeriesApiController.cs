// -----------------------------------------------------------------------
//  <copyright file="BaseTimeSeriesApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Database.Common;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.TimeSeries.Controllers
{
	public abstract class BaseTimeSeriesApiController : ResourceApiController<TimeSeriesStorage, TimeSeriesLandlord>
	{
		protected string TimeSeriesName
		{
			get { return ResourceName; }
		}

		protected TimeSeriesStorage TimeSeries
		{
			get { return Resource; }
		}

		public override ResourceType ResourceType
		{
			get { return ResourceType.TimeSeries; }
		}

		public override void MarkRequestDuration(long duration)
		{
			if (Resource == null)
				return;

			Resource.MetricsTimeSeries.RequestDurationMetric.Update(duration);
		}
	}
}