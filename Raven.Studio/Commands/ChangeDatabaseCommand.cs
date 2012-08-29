using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class ChangeDatabaseCommand : Command
	{
		private string databaseName;
		private readonly bool navigateAfter;

		public ChangeDatabaseCommand(bool navigateAfter = false)
		{
			this.navigateAfter = navigateAfter;
		}

		public ChangeDatabaseCommand()
		{
			
		}

		public override bool CanExecute(object parameter)
		{
			databaseName = parameter as string;
			return string.IsNullOrEmpty(databaseName) == false && ApplicationModel.Current.Server.Value != null;
		}

		public override void Execute(object parameter)
		{
			var urlParser = new UrlParser(UrlUtil.Url);
			if (urlParser.GetQueryParam("database") == databaseName)
			{
			    return;
			}
			if (navigateAfter) 
				urlParser = new UrlParser("/documents");

			urlParser.SetQueryParam("database", databaseName);
			// MainPage.ContentFrame_Navigated takes care of actually responding to the db name change
			UrlUtil.Navigate(urlParser.BuildUrl());
		}
	}
}