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
        private readonly int itemsPerPages;

        public DecreasePageCommand(string location)
        {
            this.location = location;
            if(location == "/Home")
            {
                itemsPerPages = 15;
            }
            else
            {
                itemsPerPages = 25;
            }
        }

        public override void Execute(object parameter)
        {
            int currentSkip = GetSkipCount() - itemsPerPages;

            ApplicationModel.Current.Navigate((new Uri(location+ "?skip=" + currentSkip, UriKind.Relative)));
        }

        public override bool CanExecute(object parameter)
        {
            return base.CanExecute(parameter);
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