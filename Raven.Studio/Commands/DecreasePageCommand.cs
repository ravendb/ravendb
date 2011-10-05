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
    public class DecreasePageCommand : Command
	{
        private readonly string location;
        private readonly int itemsPerPage;

        public DecreasePageCommand(string location, int itemsPerPage)
        {
            this.location = location;
            this.itemsPerPage = itemsPerPage;
        }

        public override void Execute(object parameter)
        {
            int currentSkip = UrlUtil.GetSkipCount() - itemsPerPage;

            ApplicationModel.Current.Navigate((new Uri(location+ "?skip=" + currentSkip, UriKind.Relative)));
        }

        public override bool CanExecute(object parameter)
        {
            if(UrlUtil.GetSkipCount() == 0)
            {
                return false;
            }
            return base.CanExecute(parameter);
        }
	}
}