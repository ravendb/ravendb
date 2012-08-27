using System;
using System.Net;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

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
			var url = "http://maps.googleapis.com/maps/api/geocode/json?address=" + queryModel.Address + "&sensor=false";
			var webRequest = WebRequest.Create(new Uri(url, UriKind.Absolute));
			webRequest.GetResponseAsync().ContinueOnSuccessInTheUIThread(doc =>
			{
				var x = doc;
			}).Catch(exception => exception = exception);

		}
	}
}
