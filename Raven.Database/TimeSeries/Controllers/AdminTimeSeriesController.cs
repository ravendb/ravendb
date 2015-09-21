// -----------------------------------------------------------------------
//  <copyright file="AdminFSController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.TimeSeries;
using Raven.Database.Extensions;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.TimeSeries.Controllers
{
    public class AdminTimeSeriesController : BaseAdminTimeSeriesApiController
    {
        [HttpPut]
		[RavenRoute("admin/ts/{*timeSeriesName}")]
		public async Task<HttpResponseMessage> Put(string timeSeriesName)
        {
			MessageWithStatusCode nameFormatErrorMessage;
			if (IsValidName(timeSeriesName, SystemConfiguration.TimeSeries.DataDirectory, out nameFormatErrorMessage) == false)
			{
				return GetMessageWithObject(new
				{
					Error = nameFormatErrorMessage.Message
				}, nameFormatErrorMessage.ErrorCode);
			}

			if (Authentication.IsLicensedForTimeSeries == false)
			{
				return GetMessageWithObject(new
				{
					Error = "Your license does not allow the use of Time Series!"
				}, HttpStatusCode.BadRequest);
			}

			var docKey = Constants.TimeSeries.Prefix + timeSeriesName;

			var isTimeSeriesUpdate = ParseBoolQueryString("update");
			var timeSeries = SystemDatabase.Documents.Get(docKey);
			if (timeSeries != null && isTimeSeriesUpdate == false)
            {
				return GetMessageWithString(string.Format("Time series {0} already exists!", timeSeriesName), HttpStatusCode.Conflict);
            }

            var dbDoc = await ReadJsonObjectAsync<TimeSeriesDocument>();
            TimeSeriesLandlord.Protect(dbDoc);
            var json = RavenJObject.FromObject(dbDoc);
            json.Remove("Id");

			SystemDatabase.Documents.Put(docKey, null, json, new RavenJObject(), null);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

		[HttpDelete]
		[RavenRoute("admin/ts/{*timeSeriesName}")]
		public HttpResponseMessage Delete(string timeSeriesName)
		{
			var isHardDeleteNeeded = ParseBoolQueryString("hard-delete");
			var message = DeleteTimeSeries(timeSeriesName, isHardDeleteNeeded);
			if (message.ErrorCode != HttpStatusCode.OK)
			{
				return GetMessageWithString(message.Message, message.ErrorCode);
			}

			return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpDelete]
		[RavenRoute("admin/ts/batch-delete")]
		public HttpResponseMessage BatchDelete()
		{
			string[] timeSeriesToDelete = GetQueryStringValues("ids");
			if (timeSeriesToDelete == null)
			{
				return GetMessageWithString("No time series to delete!", HttpStatusCode.BadRequest);
			}

			var isHardDeleteNeeded = ParseBoolQueryString("hard-delete");
			var successfullyDeletedDatabase = new List<string>();

			timeSeriesToDelete.ForEach(id =>
			{
				var message = DeleteTimeSeries(id, isHardDeleteNeeded);
				if (message.ErrorCode == HttpStatusCode.OK)
				{
					successfullyDeletedDatabase.Add(id);
				}
			});

			return GetMessageWithObject(successfullyDeletedDatabase.ToArray());
		}

		[HttpPost]
		[RavenRoute("admin/ts/{*timeSeriesName}")]
		public HttpResponseMessage Disable(string timeSeriesName, bool isSettingDisabled)
		{
			var message = ToggleTimeSeriesDisabled(timeSeriesName, isSettingDisabled);
			if (message.ErrorCode != HttpStatusCode.OK)
			{
				return GetMessageWithString(message.Message, message.ErrorCode);
			}

			return GetEmptyMessage();
		}

		[HttpPost]
		[RavenRoute("admin/ts/batch-toggle-disable")]
		public HttpResponseMessage ToggleDisable(bool isSettingDisabled)
		{
			string[] timeSeriesToToggle = GetQueryStringValues("ids");
			if (timeSeriesToToggle == null)
			{
				return GetMessageWithString("No time series to toggle!", HttpStatusCode.BadRequest);
			}

			var successfullyToggledTimeSeries = new List<string>();

			timeSeriesToToggle.ForEach(id =>
			{
				var message = ToggleTimeSeriesDisabled(id, isSettingDisabled);
				if (message.ErrorCode == HttpStatusCode.OK)
				{
					successfullyToggledTimeSeries.Add(id);
				}
			});

			return GetMessageWithObject(successfullyToggledTimeSeries.ToArray());
		}

		private MessageWithStatusCode DeleteTimeSeries(string id, bool isHardDeleteNeeded)
		{
			//get configuration even if the time series is disabled
			var configuration = TimeSeriesLandlord.CreateTenantConfiguration(id, true);

			if (configuration == null)
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "Time series wasn't found" };

			var docKey = Constants.TimeSeries.Prefix + id;
			SystemDatabase.Documents.Delete(docKey, null, null);

			if (isHardDeleteNeeded && configuration.RunInMemory == false)
			{
				IOExtensions.DeleteDirectory(configuration.TimeSeries.DataDirectory);
			}

			return new MessageWithStatusCode();
		}

		private MessageWithStatusCode ToggleTimeSeriesDisabled(string id, bool isSettingDisabled)
		{
			var docKey = Constants.TimeSeries.Prefix + id;
			var document = SystemDatabase.Documents.Get(docKey);
			if (document == null)
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "Time series " + id + " wasn't found" };

			var doc = document.DataAsJson.JsonDeserialization<TimeSeriesDocument>();
			if (doc.Disabled == isSettingDisabled)
			{
				var state = isSettingDisabled ? "disabled" : "enabled";
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.BadRequest, Message = "Time series " + id + " is already " + state };
			}

			doc.Disabled = !doc.Disabled;
			var json = RavenJObject.FromObject(doc);
			json.Remove("Id");
			SystemDatabase.Documents.Put(docKey, document.Etag, json, new RavenJObject(), null);

			return new MessageWithStatusCode();
		}
    }
}