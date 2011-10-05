using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
    public class IncreasePageCommand : Command
	{
        private readonly string location;
        private readonly int itemsPerPage;
        private readonly long numberOfPages;

        public IncreasePageCommand(string location, int itemsPerPage, long numberOfPages)
        {
            this.location = location;
            this.itemsPerPage = itemsPerPage;
            this.numberOfPages = numberOfPages;
        }

        public override void Execute(object parameter)
        {
            int currentSkip = UrlUtil.GetSkipCount() + itemsPerPage;

            ApplicationModel.Current.Navigate((new Uri(location+ "?skip=" + currentSkip, UriKind.Relative)));
        }

        public override bool CanExecute(object parameter)
        {
            if ((UrlUtil.GetSkipCount() / itemsPerPage) + 1 >= numberOfPages && UrlUtil.GetSkipCount() != 0)
                return (false);
            return true;
        }
	}
}