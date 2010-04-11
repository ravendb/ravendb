using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Database.Data
{
    public class DeleteCommandData : ICommandData
    {
        public virtual string Key { get; set; }
        public virtual Guid? Etag { get; set; }
    }
}
