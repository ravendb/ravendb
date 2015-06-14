using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.TimeSeries.Controllers
{
	public class TimeSeriesController : RavenDbApiController
	{
		[RavenRoute("ts")]
		[HttpGet]
		public HttpResponseMessage TimeSeries(bool getAdditionalData = false)
		{
			if (EnsureSystemDatabase() == false)
				return GetMessageWithString("The request '" + InnerRequest.RequestUri.AbsoluteUri + "' can only be issued on the system database", HttpStatusCode.BadRequest);

			// This method is NOT secured, and anyone can access it.
			// Because of that, we need to provide explicit security here.

			// Anonymous Access - All / Get / Admin
			// Show all file systems

			// Anonymous Access - None
			// Show only the file system that you have access to (read / read-write / admin)

			// If admin, show all file systems


			var timeSeriesDocuments = GetResourcesDocuments(Constants.TimeSeries.Prefix);
			var timeSeriesData = GetTimeSeriesData(timeSeriesDocuments);
			var timeSeriesNames = timeSeriesData.Select(x => x.Name).ToArray();

			List<string> approvedTimeSeriesStorages = null;
			if (SystemConfiguration.AnonymousUserAccessMode == AnonymousUserAccessMode.None)
			{
				var authorizer = (MixedModeRequestAuthorizer)ControllerContext.Configuration.Properties[typeof(MixedModeRequestAuthorizer)];

				HttpResponseMessage authMsg;
				if (authorizer.TryAuthorize(this, out authMsg) == false)
					return authMsg;

				var user = authorizer.GetUser(this);
				if (user == null)
					return authMsg;

				if (user.IsAdministrator(SystemConfiguration.AnonymousUserAccessMode) == false)
				{
					approvedTimeSeriesStorages = authorizer.GetApprovedResources(user, this, timeSeriesNames);
				}

				timeSeriesData.ForEach(x =>
				{
					var principalWithDatabaseAccess = user as PrincipalWithDatabaseAccess;
					if (principalWithDatabaseAccess != null)
					{
						var isAdminGlobal = principalWithDatabaseAccess.IsAdministrator(SystemConfiguration.AnonymousUserAccessMode);
						x.IsAdminCurrentTenant = isAdminGlobal || principalWithDatabaseAccess.IsAdministrator(Database);
					}
					else
					{
						x.IsAdminCurrentTenant = user.IsAdministrator(x.Name);
					}
				});
			}

			if (approvedTimeSeriesStorages != null)
			{
				timeSeriesData = timeSeriesData.Where(data => approvedTimeSeriesStorages.Contains(data.Name)).ToList();
				timeSeriesNames = timeSeriesNames.Where(name => approvedTimeSeriesStorages.Contains(name)).ToArray();
			}

			var responseMessage = getAdditionalData ? GetMessageWithObject(timeSeriesData) : GetMessageWithObject(timeSeriesNames);
			return responseMessage.WithNoCache();
		}

		private class TimeSeriesData : TenantData
		{
		}

		private static List<TimeSeriesData> GetTimeSeriesData(IEnumerable<RavenJToken> timeSeries)
		{
			return timeSeries
				.Select(ts =>
				{
					var bundles = new string[] { };
					var settings = ts.Value<RavenJObject>("Settings");
					if (settings != null)
					{
						var activeBundles = settings.Value<string>("Raven/ActiveBundles");
						if (activeBundles != null)
						{
							bundles = activeBundles.Split(';');
						}
					}
					return new TimeSeriesData
					{
						Name = ts.Value<RavenJObject>("@metadata").Value<string>("@id").Replace(Constants.TimeSeries.Prefix, string.Empty),
						Disabled = ts.Value<bool>("Disabled"),
						Bundles = bundles,
						IsAdminCurrentTenant = true,
					};
				}).ToList();
		}
	}
}