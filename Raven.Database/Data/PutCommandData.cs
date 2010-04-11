using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
    public class PutCommandData : ICommandData
    {
        public virtual string Key { get; set; }
        public virtual Guid? Etag { get; set; }
        public virtual JObject Document { get; set; }
        public virtual JObject Metadata { get; set; }
    }
}
