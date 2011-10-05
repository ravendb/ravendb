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
            int currentSkip = GetSkipCount() + itemsPerPage;

            ApplicationModel.Current.Navigate((new Uri(location+ "?skip=" + currentSkip, UriKind.Relative)));
        }

        public override bool CanExecute(object parameter)
        {
            if ((GetSkipCount() / itemsPerPage) + 1 >= numberOfPages && GetSkipCount()!=0)
                return (false);
            return true;
        }


        public int GetSkipCount()
        {
            var queryParam = ApplicationModel.Current.GetQueryParam("skip");
            if (string.IsNullOrEmpty(queryParam))
                return 0;
            int result;
            int.TryParse(queryParam, out result);
            return result;
        }
	}
}