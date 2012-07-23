// -----------------------------------------------------------------------
//  <copyright file="Base62Util.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;

namespace Raven.Client.Changes
{
	public class Base62Util
	{
		private static readonly Random random = new Random();

		public static string Base62Random()
		{
			return Base62ToString(random.Next());
		}

		private static string Base62ToString(long value)
		{
			// Divides the number by 64, so how many 64s are in 
			// 'value'. This number is stored in Y.  
			// e.g #1  
			// 1) 1000 / 62 = 16, plus 8 remainder (stored in x). 
			// 2) 16 / 62 = 0, remainder 16 
			// 3) 16, 8 or G8: 
			// 4) 65 is A, add 6 to this = 71 or G. 
			// 
			// e.g #2:  
			// 1) 10000 / 62 = 161, remainder 18 
			// 2) 161 / 62 = 2, remainder 37 
			// 3) 2 / 62 = 0, remainder 2 
			// 4) 2, 37, 18, or 2,b,I: 
			// 5) 65 is A, add 27 to this (minus 10 from 37 as these are digits) = 92. 
			//    Add 6 to 92, as 91-96 are symbols. 98 is b. 
			// 6)  
			long x = value % 62L;
			long y = value / 62L;
			if (y > 0)
				return Base62ToString(y) + ValToChar(x).ToString(CultureInfo.InvariantCulture);
			return ValToChar(x).ToString(CultureInfo.InvariantCulture);
		}

		private static char ValToChar(long value)
		{
			if (value > 9)
			{
				int ascii = (65 + ((int)value - 10));
				if (ascii > 90)
					ascii += 6;
				return (char)ascii;
			}
			else
				return value.ToString()[0];
		} 
	}
}