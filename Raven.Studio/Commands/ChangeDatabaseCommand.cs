using System;
using System.Collections.Generic;
using System.IO.IsolatedStorage;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class ChangeDatabaseCommand : Command
	{
		private string databaseName;

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

			urlParser.SetQueryParam("database", databaseName);

            // MainPage.ContentFrame_Navigated takes care of actually responding to the db name change
		    UrlUtil.Navigate(urlParser.BuildUrl());
		}
	}
}