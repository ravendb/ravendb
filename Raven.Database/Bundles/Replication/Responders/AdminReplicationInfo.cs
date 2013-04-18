// -----------------------------------------------------------------------
//  <copyright file="AdminReplicationInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders.Admin
{
    [ExportMetadata("Bundle", "Replication")]
    [InheritedExport(typeof(AbstractRequestResponder))]
    public class AdminReplicationInfo : AdminResponder
	{
		private readonly HttpRavenRequestFactory requestFactory;

		public AdminReplicationInfo()
		{
			requestFactory = new HttpRavenRequestFactory();
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}

		public override void RespondToAdmin(IHttpContext context)
		{
			
			var replicationDocument = context.ReadJsonObject<ReplicationDocument>();

			if (replicationDocument == null || replicationDocument.Destinations == null || replicationDocument.Destinations.Count == 0)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "Invalid `ReplicationDocument` document supplied."
				});

				return;
			}

			var statuses = CheckDestinations(replicationDocument);

			context.WriteJson(statuses);
		}

		private ReplicationInfoStatus[] CheckDestinations(ReplicationDocument replicationDocument)
		{
			var results = new ReplicationInfoStatus[replicationDocument.Destinations.Count];

            Parallel.ForEach(replicationDocument.Destinations, (replicationDestination,state,i) =>
			{
				var url = replicationDestination.Url;

				if (!url.ToLower().Contains("/databases/"))
				{
					url += "/databases/" + replicationDestination.Database;
				}

				var result = new ReplicationInfoStatus
				{
					Url = url,
					Status = "Valid",
					Code = (int)HttpStatusCode.OK
				};

				results[i] = result;

			    var ravenConnectionStringOptions = new RavenConnectionStringOptions
			    {
			        ApiKey = replicationDestination.ApiKey, 
                    DefaultDatabase = replicationDestination.Database,
			    };
                if (string.IsNullOrEmpty(replicationDestination.Username) == false)
                {
                    ravenConnectionStringOptions.Credentials = new NetworkCredential(replicationDestination.Username,
                                                                                     replicationDestination.Password,
                                                                                     replicationDestination.Domain ?? string.Empty);
                }
			    var request = requestFactory.Create(url + "/replication/info", "POST", ravenConnectionStringOptions);
				try
				{	
					request.ExecuteRequest();
				}
				catch (WebException e)
				{
					FillStatus(result, e);
				}
			});

			return results;
		}

		private void FillStatus(ReplicationInfoStatus replicationInfoStatus, WebException e)
		{
			var response = e.Response as HttpWebResponse;
			if (response == null)
			{
				replicationInfoStatus.Status = e.Message;
				replicationInfoStatus.Code = -1 * (int)e.Status;
				return;
			}

			switch (response.StatusCode)
			{
				case HttpStatusCode.BadRequest:
					string error = GetErrorStringFromException(e, response);
					replicationInfoStatus.Status = error.Contains("Could not figure out what to do")
					                                       ? "Replication Bundle not activated."
					                                       : error;
					    replicationInfoStatus.Code = (int) response.StatusCode;
			        break;
                case HttpStatusCode.PreconditionFailed:
			        replicationInfoStatus.Status = "Could not authenticate using OAuth's API Key";
			        replicationInfoStatus.Code = (int) response.StatusCode;
			        break;
                case HttpStatusCode.Forbidden:
                case HttpStatusCode.Unauthorized:
			        replicationInfoStatus.Status = "Could not authenticate using Windows Auth";
			        replicationInfoStatus.Code = (int) response.StatusCode;
			        break;
				default:
					replicationInfoStatus.Status = response.StatusDescription;
					replicationInfoStatus.Code = (int)response.StatusCode;
					break;
			}
		}

	    private static string GetErrorStringFromException(WebException webException, HttpWebResponse response)
	    {
		    var s = webException.Data["original-value"] as string;
		    if (s != null)
			    return s;
		    using (var streamReader = new StreamReader(response.GetResponseStreamWithHttpDecompression()))
		    {
			    return streamReader.ReadToEnd();
		    }
	    }

	    private static void GetValue(HttpWebResponse response)
	    {
		    using (var streamReader = new StreamReader(response.GetResponseStreamWithHttpDecompression()))
		    {
			    var error = streamReader.ReadToEnd();
		    }
	    }
	}
}