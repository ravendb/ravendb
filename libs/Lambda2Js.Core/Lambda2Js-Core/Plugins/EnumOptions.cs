using System;

namespace Lambda2Js
{
    [Flags]
    public enum EnumOptions
    {
        /// <summary>
        /// Represents enums of flags as the summed result.
        /// </summary>
        FlagsAsResultingSum = 0x80,

        /// <summary>
        /// Represents enums of flags as A|B|C
        /// </summary>
        FlagsAsNumericOrs = 0x40,

        /// <summary>
        /// Represents enums of flags as "A|B|C"
        /// </summary>
        FlagsAsStringWithSeparator = 0x20,

        /// <summary>
        /// Represents enums of flags as arrays.
        /// If combined with UseStrings: ["A","B","C"]
        /// If combined with UseStaticFields: [E.A,E.B,E.C]
        /// If combined with UseNumbers: [1,2,3]
        /// </summary>
        FlagsAsArray = 0x10,

        /// <summary>
        /// Represents enums with their names i.e. "A"
        /// </summary>
        UseStrings = 2,

        /// <summary>
        /// Represents enums with their names i.e. E.A
        /// </summary>
        UseStaticFields = 4,

        /// <summary>
        /// Represents enums with their names i.e. 1
        /// </summary>
        UseNumbers = 1,
    }
}