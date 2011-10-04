using System;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Indexes
{
	public class IndexDefinitionModelLocator : ModelLocatorBase<IndexDefinitionModel>
	{
		protected override void Load(IAsyncDatabaseCommands asyncDatabaseCommands, Observable<IndexDefinitionModel> observable)
		{
			var name = GetParamAfter("/indexes/");
			if (name == null)
				return;

			asyncDatabaseCommands.GetIndexAsync(name)
				.ContinueOnSuccess(index =>
				{
					if (index == null)
					{
						ApplicationModel.Current.Navigate(new Uri("/NotFound?id=" + name, UriKind.Relative));
						return;
					}
					observable.Value = new IndexDefinitionModel(index, asyncDatabaseCommands, ServerModel.SelectedDatabase.Value.Statistics);
				}
				)
				.Catch();
		}

		private string GetParamAfter(string urlPrefix)
		{
			var url = ApplicationModel.Current.NavigationState;
			if (url.StartsWith(urlPrefix) == false)
				return null;

			return url.Substring(urlPrefix.Length);
		}
	}
}