using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Revisions
{
    public class EnforceRevisionsConfigurationRequest
    {
        public bool IncludeForceCreated { get; set; } = false;
        public string[] Collections { get; set; } = null;
    }
}
