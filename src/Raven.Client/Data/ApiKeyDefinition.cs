using System.Collections.Generic;
using System.Linq;
using  Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{
    public class ResourceAccess
    {        
        /// <summary>
        /// Id a database.
        /// </summary>
        public string TenantId { get; set; }

        /// <summary>
        /// Indicates if read-only / Admin acesss should be granted.
        /// </summary>
        public string AccessMode { get; set; }
    }





}
