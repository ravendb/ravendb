using System;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
	public class NavigateToDocumentCommand : Command
	{
		private string key;

		public override bool CanExecute(object parameter)
		{
			key = parameter as string;
			return string.IsNullOrEmpty(key);
		}

		public override void Execute(object parameter)
		{
			ApplicationModel.Current.Navigate(new Uri("/Edit?id=" + key, UriKind.Relative));
		}
	}
}