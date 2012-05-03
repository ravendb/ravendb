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
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.Windows.Threading;

namespace Raven.Studio.Infrastructure
{
    public class RecyclingContentLoader : INavigationContentLoader
    {
        PageResourceContentLoader innerLoader = new PageResourceContentLoader();
        private UserControl previousView;

        public IAsyncResult BeginLoad(Uri targetUri, Uri currentUri, AsyncCallback userCallback, object asyncState)
        {
            if (currentUri != null)
            {
                var targetPage = GetFileName(targetUri.OriginalString);
                var currentPage = GetFileName(currentUri.OriginalString);

                if (targetPage == currentPage)
                {
                    var task = new TaskCompletionSource<bool>(asyncState);
                    Application.Current.RootVisual.Dispatcher.BeginInvoke(() => userCallback(task.Task));

                    return task.Task;
                }
            }
            return innerLoader.BeginLoad(targetUri, currentUri, userCallback, asyncState);
        }

        public void CancelLoad(IAsyncResult asyncResult)
        {
            
        }

        public LoadResult EndLoad(IAsyncResult asyncResult)
        {
            if (asyncResult is Task<bool>)
            {
                return new LoadResult(previousView);
            }
            else
            {
                var result = innerLoader.EndLoad(asyncResult);
                previousView = result.LoadedContent as UserControl;
                return result;
            }
        }

        public bool CanLoad(Uri targetUri, Uri currentUri)
        {
            return innerLoader.CanLoad(targetUri, currentUri);
        }

        private string GetFileName(string fullPath)
        {
            var indexOfPeriod = fullPath.IndexOf('.');
            if (indexOfPeriod < 0)
            {
                return fullPath;
            }
            else
            {
                return fullPath.Substring(0, indexOfPeriod);
            }
        }
    }
}
