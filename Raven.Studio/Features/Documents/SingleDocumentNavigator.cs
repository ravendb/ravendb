using System;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
    public class SingleDocumentNavigator : DocumentNavigator
    {
        private string id;

        public SingleDocumentNavigator(string id)
        {
            this.id = id;
        }

        public override string GetUrl()
        {
            var urlBuilder = GetBaseUrl();

            urlBuilder.SetQueryParam("id", id);

            return urlBuilder.BuildUrl();
        }

        public override Task<DocumentAndNavigationInfo> GetDocument()
        {
            return DatabaseCommands.GetAsync(id).ContinueWith(t => new DocumentAndNavigationInfo()
                                                                       {
                                                                           Document = t.Result,
                                                                           TotalDocuments = 1,
                                                                           Index = 0,
                                                                       });
        }
    }
}
