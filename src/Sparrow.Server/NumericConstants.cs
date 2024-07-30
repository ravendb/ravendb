using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow
{
    /// <summary>
    /// The objective of the INumericConstant is to actually be able to use the derived types to craft
    /// highly efficient code, since those type values are going to be constant.
    /// </summary>
    public interface INumericConstant
    {
        int N { get; }
    }

    internal struct N1 : INumericConstant
    {
        public int N => 1;
    }

    internal struct N2 : INumericConstant
    {
        public int N => 2;
    }

    internal struct N4 : INumericConstant
    {
        public int N => 4;
    }
}
