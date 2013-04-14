// -----------------------------------------------------------------------
//  <copyright file="AdminReplicationInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
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
			if (Database == SystemDatabase)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "Cannot check replication against `system` database."
				});

				return;
			}

			var replicationDocument = context.ReadJsonObject<ReplicationDocument>();

			if (replicationDocument == null)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "Invalid `ReplicationDocument` document supplied."
				});

				return;
			}

			var databaseReplicationDocument = Database.Get("Raven/Replication/Destinations", null);
			if (databaseReplicationDocument == null)
			{
				context.WriteJson(new
				{
					Error = "`ReplicationDocument` is not available on " + Database.ServerUrl + "/databases/" + Database.Name
				});
				return;
			}

			var currentReplicationDocument = databaseReplicationDocument.DataAsJson.JsonDeserialization<ReplicationDocument>();
			if (currentReplicationDocument == null)
			{
				context.WriteJson(new
				{
					Error = "`ReplicationDocument` is invalid on " + Database.ServerUrl + "/databases/" + Database.Name
				});
				return;
			}

			var statuses = CheckDestinations(currentReplicationDocument, replicationDocument);

			context.WriteJson(statuses);
		}

		private List<ReplicationInfoStatus> CheckDestinations(ReplicationDocument currentReplicationDocument, ReplicationDocument replicationDocument)
		{
			var results = new List<ReplicationInfoStatus>();

			Parallel.ForEach(replicationDocument.Destinations, replicationDestination =>
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

				results.Add(result);

				var knownDestination =
					currentReplicationDocument.Destinations.Any(x => x.Url.ToLower() == replicationDestination.Url.ToLower());

				if (!knownDestination)
				{
					result.Status = "Unknown destination.";
					result.Code = -1;
					return;
				}

				var request = requestFactory.Create(url + "/replication/replicateDocs", "POST", new RavenConnectionStringOptions
				{
					ApiKey = replicationDestination.ApiKey,
					DefaultDatabase = replicationDestination.Database
				});

				try
				{
					request.Write(new RavenJObject());
					using (request.WebRequest.GetResponse())
					{
					}
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
				replicationInfoStatus.Code = (int)e.Status;
				return;
			}

			switch (response.StatusCode)
			{
				case HttpStatusCode.BadRequest:
					using (var streamReader = new StreamReader(response.GetResponseStreamWithHttpDecompression()))
					{
						var error = streamReader.ReadToEnd();
						if (!string.IsNullOrEmpty(error)) // if 'empty' then response from replication responder
						{
							replicationInfoStatus.Status = error.Contains("Could not figure out what to do") ? "Replication Bundle not activated." : error;
							replicationInfoStatus.Code = (int)response.StatusCode;
						}
					}
					break;
				default:
					replicationInfoStatus.Status = response.StatusDescription;
					replicationInfoStatus.Code = (int)response.StatusCode;
					break;
			}
		}

		private class ReplicationInfoStatus
		{
			public string Url { get; set; }

			public string Status { get; set; }

			public int Code { get; set; }
		}
	}
}