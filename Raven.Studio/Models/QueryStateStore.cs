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
using Raven.Studio.Features.Query;

namespace Raven.Studio.Models
{
    public class QueryStateStore
    {
        private Dictionary<string,QueryState> _indexQueryStates = new Dictionary<string, QueryState>();
    }
}
