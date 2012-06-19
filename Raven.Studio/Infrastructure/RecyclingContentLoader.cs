using System;
using System.Net;
using System.Threading;
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
    public class RecyclingContentLoader : DependencyObject, INavigationContentLoader
    {
        PageResourceContentLoader innerLoader = new PageResourceContentLoader();
        private TaskScheduler scheduler;

        public static readonly DependencyProperty ParentFrameProperty =
            DependencyProperty.Register("ParentFrame", typeof(Frame), typeof(RecyclingContentLoader), new PropertyMetadata(default(Frame)));

        public Frame ParentFrame
        {
            get { return (Frame)GetValue(ParentFrameProperty); }
            set { SetValue(ParentFrameProperty, value); }
        }

        public RecyclingContentLoader()
        {
            scheduler = TaskScheduler.FromCurrentSynchronizationContext();
        }

        public IAsyncResult BeginLoad(Uri targetUri, Uri currentUri, AsyncCallback userCallback, object asyncState)
        {
            if (currentUri != null)
            {
                var targetPage = GetFileName(targetUri.OriginalString);
                var currentPage = GetFileName(currentUri.OriginalString);

                if (targetPage == currentPage)
                {
                    var tcs = new TaskCompletionSource<UserControl>(asyncState);

                    Task.Factory.StartNew(() =>
                                              {
                                                  tcs.SetResult(ParentFrame.Content as UserControl);
                                                  userCallback(tcs.Task);
                                              }, CancellationToken.None, TaskCreationOptions.None, scheduler);

                    return tcs.Task;
                }
            }
            return innerLoader.BeginLoad(targetUri, currentUri, userCallback, asyncState);
        }

        public void CancelLoad(IAsyncResult asyncResult)
        {
            
        }

        public LoadResult EndLoad(IAsyncResult asyncResult)
        {
            if (asyncResult is Task<UserControl>)
            {
                return new LoadResult((asyncResult as Task<UserControl>).Result);
            }
            else
            {
                var result = innerLoader.EndLoad(asyncResult);
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
