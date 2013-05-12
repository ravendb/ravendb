using System;
using System.IO;
using System.Net;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using System.Linq;

namespace Raven.Studio.Commands
{
	public class CalculateGeocodeFromAddressCommand : Command
	{
		private readonly SpatialQueryModel queryModel;

		public CalculateGeocodeFromAddressCommand(SpatialQueryModel queryModel)
		{
			this.queryModel = queryModel;
		}

		public override void Execute(object parameter)
		{
			if (string.IsNullOrWhiteSpace(queryModel.Address))
				return;

			var url = "http://dev.virtualearth.net/REST/v1/Locations?q=" + Uri.EscapeUriString(queryModel.Address) +
					  "&key=Anlj2YMQu676uXmSj1QTSni66f8DjuBGToZ21t5z9E__lL8IHRhFP8LtF7umitL6";
			var webRequest = WebRequest.Create(new Uri(url, UriKind.Absolute));
			webRequest.GetResponseAsync().ContinueOnSuccessInTheUIThread(doc =>
			{
				RavenJObject jsonData;
				using (var stream = doc.GetResponseStream())
				using (var reader = new StreamReader(stream))
				using (var jsonReader = new JsonTextReader(reader))
					jsonData = RavenJObject.Load(jsonReader);

				var set = jsonData["resourceSets"];

				var item = set.Values().First().Values().ToList()[1].Values().ToList();
				if (item.Count == 0)
				{
					ApplicationModel.Current.AddInfoNotification("Could not calculate the given address");
					return;
				}

				var result = item.First().SelectToken("point").SelectToken("coordinates").Values().ToList();

				if (result != null)
				{
					var latitude = double.Parse(result[0].ToString());
					var longitude = double.Parse(result[1].ToString());
					var addressData = new AddressData { Address = queryModel.Address, Latitude = latitude, Longitude = longitude };
					queryModel.UpdateResultsFromCalculate(addressData);
				}

			}).Catch();
		}
	}
}