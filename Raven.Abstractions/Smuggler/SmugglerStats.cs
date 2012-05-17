using System;

namespace Raven.Abstractions.Smuggler
{
    public class SmugglerStats
    {
        public int Indexes { get; set; }
        public int Documents { get; set; }
        public int Attachments { get; set; }
        public TimeSpan Elapsed { get; set; }

        public override string ToString()
        {
            return string.Format("Indexes: {0}, Documents: {1}, Attachments: {2}, Elapsed: {3}", Indexes, Documents, Attachments, Elapsed);
        }
    }
}