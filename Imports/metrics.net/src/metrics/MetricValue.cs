using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace Metrics
{
    interface IMetricValue:IDisposable
    {
        string Name { get;}
    }
}
