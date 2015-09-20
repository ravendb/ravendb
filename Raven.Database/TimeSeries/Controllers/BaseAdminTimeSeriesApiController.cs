// -----------------------------------------------------------------------
//  <copyright file="BaseAdminTimeSeriesApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Database.Common;
using Raven.Database.Config;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.TimeSeries.Controllers
{
	public class BaseAdminTimeSeriesApiController : AdminResourceApiController<TimeSeriesStorage, TimeSeriesLandlord>
	{
		public override InMemoryRavenConfiguration ResourceConfiguration
		{
			get
			{
				throw new NotSupportedException("Use TimeSeries.Configuration instead.");
			}
		}

		public string TimeSeriesName => ResourceName;

		public TimeSeriesStorage TimeSeries => Resource;

		public override ResourceType ResourceType => ResourceType.TimeSeries;

		public override void MarkRequestDuration(long duration)
		{
			if (TimeSeries == null)
				return;
			TimeSeries.MetricsTimeSeries.RequestDurationMetric.Update(duration);
		}
	}
}