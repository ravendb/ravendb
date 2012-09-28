using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Native {
    /// <summary>
    /// A property can have zero or more attributes from the following set
    /// </summary>
    [Flags]
    public enum PropertyAttributes {
        None = 0,

        /// <summary>
        /// The property is a read-only property. Attempts by ECMAScript code to write to
        /// the property will be ignored. (Note, however, that in some cases the value of a
        /// property with the ReadOnly attribute may change over time because of actions
        /// taken by the host environment; therefore “ReadOnly” does not mean “constant
        /// and unchanging”!)
        /// </summary>
        ReadOnly = 1,

        /// <summary>
        /// The property is not to be enumerated by a for-in enumeration (section 12.6.4).
        /// </summary>
        DontEnum = 2,

        /// <summary>
        /// Attempts to delete the property will be ignored. See the description of the
        /// delete operator in section 11.4.1.
        /// </summary>
        DontDelete = 4
    }
}
