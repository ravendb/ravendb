// -----------------------------------------------------------------------
//  <copyright file="BaseCountersApiController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Web.Http.Controllers;

using Raven.Database.Common;
using Raven.Database.Config;
using Raven.Database.Server.Controllers.Admin;

namespace Raven.Database.Counters.Controllers
{
	public abstract class BaseAdminCountersApiController : BaseAdminDatabaseApiController
	{
		public override InMemoryRavenConfiguration ResourceConfiguration
		{
			get
			{
				throw new NotSupportedException();
			}
		}

		public override DocumentDatabase Database
		{
			get
			{
				throw new NotSupportedException();
			}
		}

		public override string DatabaseName
		{
			get
			{
				throw new NotSupportedException();
			}
		}

		public string CounterName { get; private set; }

		private CounterStorage _counters;
		public CounterStorage Counters
		{
			get
			{
				if (_counters != null)
					return _counters;

				var resource = CountersLandlord.GetResourceInternal(CounterName);
				if (resource == null)
				{
					throw new InvalidOperationException("Could not find a counter named: " + CounterName);
				}

				return _counters = resource.Result;
			}
		}

		protected override void InnerInitialization(HttpControllerContext controllerContext)
		{
			base.InnerInitialization(controllerContext);

			CounterName = GetResourceName(controllerContext, ResourceType.Counter);
		}

		public override void MarkRequestDuration(long duration)
		{
			if (Counters == null)
				return;
			Counters.MetricsCounters.RequestDurationMetric.Update(duration);
		}
	}
}