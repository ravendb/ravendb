using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Voron.Impl
{
    public interface ICommittable
    {
        bool RequiresParticipation { get; }
        void PrepareForCommit();
    }
}
