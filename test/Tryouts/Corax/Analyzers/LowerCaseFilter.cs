// -----------------------------------------------------------------------
//  <copyright file="a.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Text;
using Raven.Server.Json;

namespace Tryouts.Corax.Analyzers
{
    public class LowerCaseFilter : IFilter
    {
        private readonly Encoding _encoding;

        public LowerCaseFilter()
        {
            _encoding = Encoding.UTF8;
        }

        public unsafe bool ProcessTerm(LazyStringValue source)
        {
            for (int i = 0; i < source.Size; i++)
            {
                var singleByteChar = (source.Buffer[i] & 0x80) != 0;
                if (singleByteChar)
                {
                    if (source.Buffer[i] >= 0x61 && source.Buffer[i] <= 0x7a)
                        source.Buffer[i] ^= 0x20;
                }
            }
            return true;
        }
    }
}