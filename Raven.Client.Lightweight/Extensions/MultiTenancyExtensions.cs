using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using Raven.Database.Data;

namespace Raven.Client.Extensions
{
    ///<summary>
    /// Extension methods to create multi tenants databases
    ///</summary>
    public static class MultiTenancyExtensions
    {
        ///<summary>
        /// Ensures that the database exists, creating it if needed
        ///</summary>
        public static void EnsureDatabaseExists(this IDatabaseCommands self,string name)
        {
            var doc = JObject.FromObject(new DatabaseDocument
            {
                Settings =
                    {
                        {"Raven/DataDir", Path.Combine("~", Path.Combine("Tenants", name))}
                    }
            });
            var docId = "Raven/Databases/" + name;
            if (self.Get(docId) != null)
                return;

            self.Put(docId, null, doc, new JObject());
        }
    }
}