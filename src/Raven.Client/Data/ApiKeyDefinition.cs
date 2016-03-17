using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Raven.Client.Data
{
    public class ApiKeyDefinition
    {
        public bool Enabled;
        public string Secret;
        public Dictionary<string, AccessModes> ResourcesAccessMode = new Dictionary<string, AccessModes>(StringComparer.OrdinalIgnoreCase);
    }


    public enum AccessModes
    {
        None,
        ReadOnly,
        ReadWrite,
        Admin
    }
}
