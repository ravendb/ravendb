// -----------------------------------------------------------------------
//  <copyright file="DocDbExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Extensions
{
	public static class DocDbExtensions
	{
		public static void AddAlert(this DocumentDatabase self,Alert alert)
		{
			while (true)
			{
				AlertsDocument alertsDocument;
				var alertsDoc = self.Get(Constants.RavenAlerts, null);
				RavenJObject metadata;
				Etag etag;
				if (alertsDoc == null)
				{
					etag = Etag.Empty;
					alertsDocument = new AlertsDocument();
					metadata = new RavenJObject();
				}
				else
				{
					etag = alertsDoc.Etag;
					alertsDocument = alertsDoc.DataAsJson.JsonDeserialization<AlertsDocument>() ?? new AlertsDocument();
					metadata = alertsDoc.Metadata;
				}

				var withSameUniqe = alertsDocument.Alerts.FirstOrDefault(alert1 => alert1.UniqueKey == alert.UniqueKey);
				if (withSameUniqe != null)
					alertsDocument.Alerts.Remove(withSameUniqe);

				alertsDocument.Alerts.Add(alert);
				var document = RavenJObject.FromObject(alertsDocument);
				document.Remove("Id");
				try
				{
					self.Put(Constants.RavenAlerts, etag, document, metadata, null);
					return;
				}
				catch (ConcurrencyException)
				{
					//try again...
				}
			}
		}
	}
}