// -----------------------------------------------------------------------
//  <copyright file="BaseAdminTimeSeriesApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Web.Http.Controllers;

using Raven.Database.Common;
using Raven.Database.Config;
using Raven.Database.Server.Controllers.Admin;

namespace Raven.Database.TimeSeries.Controllers
{
	public class BaseAdminTimeSeriesApiController : BaseAdminDatabaseApiController
	{
		public override InMemoryRavenConfiguration ResourceConfiguration
		{
			get
			{
				throw new NotSupportedException("Use TimeSeries.Configuration instead.");
			}
		}

		public override DocumentDatabase Database
		{
			get
			{
				throw new NotSupportedException("Use SystemDatabase instead.");
			}
		}

		public override string DatabaseName
		{
			get
			{
				throw new NotSupportedException();
			}
		}

		public string TimeSeriesName { get; private set; }

		private TimeSeriesStorage _timeSeries;
		public TimeSeriesStorage TimeSeries
		{
			get
			{
				if (_timeSeries != null)
					return _timeSeries;

				var resource = TimeSeriesLandlord.GetResourceInternal(TimeSeriesName);
				if (resource == null)
				{
					throw new InvalidOperationException("Could not find a time series named: " + TimeSeriesName);
				}

				return _timeSeries = resource.Result;
			}
		}

		protected override void InnerInitialization(HttpControllerContext controllerContext)
		{
			base.InnerInitialization(controllerContext);

			TimeSeriesName = GetResourceName(controllerContext, ResourceType.TimeSeries);
		}

		public override ResourceType ResourceType
		{
			get
			{
				return ResourceType.TimeSeries;
			}
		}

		public override void MarkRequestDuration(long duration)
		{
			if (TimeSeries == null)
				return;
			TimeSeries.MetricsTimeSeries.RequestDurationMetric.Update(duration);
		}
	}
}