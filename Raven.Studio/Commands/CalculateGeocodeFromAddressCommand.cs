using System;
using System.IO;
using System.Net;
using System.Text;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using System.Linq;

namespace Raven.Studio.Commands
{
	public class CalculateGeocodeFromAddressCommand : Command
	{
		private readonly QueryModel queryModel;
		public CalculateGeocodeFromAddressCommand(QueryModel queryModel)
		{
			this.queryModel = queryModel;
		}
		public override void Execute(object parameter)
		{
			var url = "http://where.yahooapis.com/geocode?flags=JC&q=" + queryModel.Address;
			var webRequest = WebRequest.Create(new Uri(url, UriKind.Absolute));
			webRequest.GetResponseAsync().ContinueOnSuccessInTheUIThread(doc =>
			{
				RavenJObject jsonData;
				using (var stream = doc.GetResponseStream())
				{
					jsonData = RavenJObject.Load(new JsonTextReader(new StreamReader(stream)));
				}
				var result = jsonData["ResultSet"].SelectToken("Results").Values().FirstOrDefault();
				if (result != null)
				{
					queryModel.Latitude = double.Parse(result.Value<string>("latitude"));
					queryModel.Longitude = double.Parse(result.Value<string>("longitude"));
				}

			}).Catch();
		}
	}
}