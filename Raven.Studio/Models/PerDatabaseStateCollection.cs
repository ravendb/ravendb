using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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
    public class PerDatabaseStateCollection
    {
        private Dictionary<string, PerDatabaseState> states = new Dictionary<string, PerDatabaseState>();
  
        public PerDatabaseState this[string databaseName]
        {
            get
            {
                PerDatabaseState state;
                if (!states.ContainsKey(databaseName))
                {
                    state = new PerDatabaseState(databaseName);
                    states.Add(databaseName, state);
                }

                return states[databaseName];
            }
        }

        public PerDatabaseState this[DatabaseModel database]
        {
            get { return this[database.Name]; }
        }
    }
}
