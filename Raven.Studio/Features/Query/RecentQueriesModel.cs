using System;
using System.Collections.ObjectModel;
using System.Net;
using System.Reactive.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Microsoft.Expression.Interactivity.Core;
using Raven.Studio.Infrastructure;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Studio.Models;
using Raven.Studio.Extensions;

namespace Raven.Studio.Features.Query
{
    public class RecentQueriesModel : ViewModel
    {
        private ICommand goToQuery;
        private ICommand pinQuery;
        private ICommand clearHistory;
        private ICommand unpinQuery;
        public ObservableCollection<SavedQuery> RecentQueries { get; private set; }
        public ObservableCollection<SavedQuery> PinnedQueries { get; private set; }

        public event EventHandler<EventArgs> QuerySelected;

        public RecentQueriesModel()
        {
            RecentQueries = new ObservableCollection<SavedQuery>();
            PinnedQueries = new ObservableCollection<SavedQuery>();    
        }

        public ICommand GoToQuery
        {
            get { return goToQuery ?? (goToQuery = new ActionCommand(HandleGoToQuery)); }
        }

        public ICommand PinQuery
        {
            get { return pinQuery ?? (pinQuery = new ActionCommand(HandlePinQuery)); }
        }

        public ICommand UnPinQuery
        {
            get { return unpinQuery ?? (unpinQuery = new ActionCommand(HandleUnPinQuery)); }
        }

        public ICommand ClearHistory
        {
            get { return clearHistory ?? (clearHistory = new ActionCommand(() => PerDatabaseState.QueryHistoryManager.ClearHistory())); }
        }

        protected override void OnViewLoaded()
        {
            Observable.FromEventPattern<EventArgs>(
                h => PerDatabaseState.QueryHistoryManager.QueriesChanged += h,
                h => PerDatabaseState.QueryHistoryManager.QueriesChanged -= h)
                .TakeUntil(Unloaded)
                .SubscribeWeakly(this, (target, args) => target.LoadQueries());

            LoadQueries();
        }

        private void LoadQueries()
        {
            RecentQueries.Clear();
            RecentQueries.AddRange(PerDatabaseState.QueryHistoryManager.RecentQueries.Where(q => !q.IsPinned));

            PinnedQueries.Clear();
            PinnedQueries.AddRange(PerDatabaseState.QueryHistoryManager.RecentQueries.Where(q => q.IsPinned).OrderBy(q => q.IndexName).ThenBy(q => q.Query));
        }

        private void HandlePinQuery(object param)
        {
            var query = param as SavedQuery;
            if (query == null)
            {
                return;
            }

            PerDatabaseState.QueryHistoryManager.PinQuery(query);
        }

        private void HandleUnPinQuery(object param)
        {
            var query = param as SavedQuery;
            if (query == null)
            {
                return;
            }

            PerDatabaseState.QueryHistoryManager.UnPinQuery(query);
        }

        private void HandleGoToQuery(object param)
        {
            var query = param as SavedQuery;
            if (query == null)
            {
                return;
            }

            string url;
            if (query.IndexName.StartsWith("dynamic/"))
            {
                var collection = query.IndexName.Substring("dynamic/".Length);
                url = string.Format("/query/?mode=dynamic&collection={0}&recentQuery={1}", Uri.EscapeDataString(collection), query.Hashcode);
            }
            else
            {
                url = string.Format("/query/{0}?recentQuery={1}", Uri.EscapeDataString(query.IndexName), query.Hashcode);
            }

            UrlUtil.Navigate(url);

            OnQuerySelected(EventArgs.Empty);
        }


        protected void OnQuerySelected(EventArgs e)
        {
            EventHandler<EventArgs> handler = QuerySelected;
            if (handler != null) handler(this, e);
        }
    }
}
