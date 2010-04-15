using System;

namespace Raven.Database
{
    public class PutResult
    {
        public string Key { get; set; }
        public Guid ETag { get; set; }
    }
}