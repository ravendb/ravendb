using System.Collections.Generic;

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