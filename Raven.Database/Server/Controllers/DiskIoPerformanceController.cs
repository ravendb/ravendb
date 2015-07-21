// -----------------------------------------------------------------------
//  <copyright file="DiskIoPerformanceController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net;
using System.Net.Http;
using System.Web.Http;

using Raven.Database.DiskIO;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using System.Net.Http.Headers;

namespace Raven.Database.Server.Controllers
{
	public class DiskIoPerformanceController : RavenDbApiController
	{
		[HttpGet]
		[RavenRoute("disk-io-performance/events")]
		public HttpResponseMessage DiskIoPerformance()
		{
			var monitor = (DiskIoPerformanceMonitor)Configuration.Properties[typeof(DiskIoPerformanceMonitor)];
			var transport = new DiskIoPerformancePushContent();
			transport.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");

			var oneTimetokenPrincipal = User as MixedModeRequestAuthorizer.OneTimetokenPrincipal;
			if ((oneTimetokenPrincipal != null && oneTimetokenPrincipal.IsAdministratorInAnonymouseMode) || SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.Admin)
				monitor.RegisterForAllResources(transport);
			else
				return GetMessageWithObject(new { Error = "Administrator user is required in order to trace disk IO for all databases" }, HttpStatusCode.Forbidden);

			return new HttpResponseMessage { Content = transport };
		}
	}
}