using System;
using System.Collections.Generic;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Models
{
    public class QueryStateStore
    {
        private Dictionary<string,QueryState> _indexQueryStates = new Dictionary<string, QueryState>();

        public QueryState GetState(string indexName)
        {
            QueryState state;
            if (!_indexQueryStates.TryGetValue(indexName, out state))
            {
                state = new QueryState();
                _indexQueryStates.Add(indexName, state);
            }

            return state;
        }
    }
}
