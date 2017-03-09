using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Raven.Client.Server.Operations
{
    /// <summary>
    /// The result of a disable or enable database
    /// </summary>
    public class DisableDatabaseToggleResult
    {
        /// <summary>
        ///  If database disabled.
        /// </summary>
        public bool Disabled;
        /// <summary>
        ///  Name of the database.
        /// </summary>
        public string Name;
        /// <summary>
        ///  If request succeed.
        /// </summary>
        public bool Success;
    }
}
