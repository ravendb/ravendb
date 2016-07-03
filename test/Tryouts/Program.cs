using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using FastTests.Blittable;

namespace Tryout
{
    class Program
    {
        static void Main(string[] args)
        {
            new BlittableValidationTest().Valid_String_with_Esc_Char();
        }
    }
}