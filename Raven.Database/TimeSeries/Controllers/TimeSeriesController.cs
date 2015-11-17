using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.TimeSeries.Controllers
{
    public class TimeSeriesController : BaseDatabaseApiController
    {
        [RavenRoute("ts")]
        [HttpGet]
        public HttpResponseMessage TimeSeries(bool getAdditionalData = false)
        {
            return Resources<TimeSeriesData>(Constants.TimeSeries.Prefix, GetTimeSeriesData, getAdditionalData);
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
                        var activeBundles = settings.Value<string>(RavenConfiguration.GetKey(x => x.Core._ActiveBundlesString));
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
