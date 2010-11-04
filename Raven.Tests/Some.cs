using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.Tests
{
    public class Some
    {
        static int index = 0;

        static public int Integer()
        {
            return index++;
        }

        static public string String()
        {
            return "someString" + Integer().ToString();
        }
    }
}
