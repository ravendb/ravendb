using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
    public class EditVirtualDocumentCommand : Command
    {
        public Func<string> NavigationQueryGenerator { get; set; }

        public override bool CanExecute(object parameter)
        {
            var document = parameter as VirtualItem<ViewableDocument>;

            return document != null;
        }

        public override void Execute(object parameter)
        {
            var virtualItem = parameter as VirtualItem<ViewableDocument>;
            if (virtualItem == null || !virtualItem.IsRealized)
            {
                return;
            }

            var viewableDocument = virtualItem.Item;

            var urlParser = new UrlParser("/edit");

            if (!string.IsNullOrEmpty(viewableDocument.Id))
            {
                urlParser.SetQueryParam("id", viewableDocument.Id);
            }

            if (NavigationQueryGenerator != null)
            {
                urlParser.SetQueryParam("index", virtualItem.Index);
                urlParser.SetQueryParam("navigationQuery", NavigationQueryGenerator());
            }

            UrlUtil.Navigate(urlParser.BuildUrl());

        }
    }
}
