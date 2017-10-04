using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.MEF
{
    public class DisableTriggerState
    {
        /// <summary>
        /// Indicates if the triggers should be disabled or not
        /// </summary>
        public bool Disabled { get; set; }
        /// <summary>
        /// Exclution hashset of types of triggers that may run even though the triggers are disabled
        /// </summary>
        public HashSet<Type> Except { get; set; }
    }
}
