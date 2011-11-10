using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class ChangeDatabaseCommand : Command
	{
		private string databaseName;

		public override bool CanExecute(object parameter)
		{
			databaseName = parameter as string;
			return string.IsNullOrEmpty(databaseName) == false;
		}

		public override void Execute(object parameter)
		{
			var urlParser = new UrlParser(UrlUtil.Url);
			urlParser.SetQueryParam("database", databaseName);
			UrlUtil.Navigate(urlParser.BuildUrl());
		}
	}
}