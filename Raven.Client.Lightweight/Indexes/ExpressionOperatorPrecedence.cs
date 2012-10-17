//-----------------------------------------------------------------------
// <copyright file="ExpressionOperatorPrecedence.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.Indexes
{
	/// <summary>
	/// Precedence values for operations
	/// </summary>
	public enum ExpressionOperatorPrecedence
	{
		/// <summary>
		/// x.y  f(x)  a[x]  x++  x--  new
		/// </summary>
		Primary = 150,
		/// <summary>
		/// +  -  !  ~  ++x  --x  (T)x
		/// </summary>
		Unary = 140,                  
		/// <summary>
		/// *  /  %
		/// </summary>
		Multiplicative = 130,         
		/// <summary>
		/// +  -
		/// </summary>
		Additive = 120,
		/// <summary>
		/// &lt;&lt; &gt;&gt;
		/// </summary>
		Shift = 110,    
		/// <summary>
		/// &lt;  &gt;  &lt;=  &gt;=  is  as
		/// </summary>
		RelationalAndTypeTesting = 100,             
		/// <summary>
		/// ==  !=
		/// </summary>
		Equality = 90,               
		/// <summary>
		/// &amp;
		/// </summary>
		LogicalAND = 80,             
		/// <summary>
		/// ^
		/// </summary>
		LogicalXOR = 70,             
		/// <summary>
		/// |
		/// </summary>
		LogicalOR = 60,
		/// <summary>
		/// &amp;&amp; (AndAlso in VB)
		/// </summary>
		ConditionalAND = 50,
		/// <summary>
		/// ||
		/// </summary>
		ConditionalOR = 40,
		/// <summary>
		/// ??
		/// </summary>
		NullCoalescing = 30,
		/// <summary>
		/// ?:
		/// </summary>
		Conditional = 20,            
		/// <summary>
		/// =  *=  /=  %=  +=  -=  &lt;&lt;=  &gt;&gt;=  &amp;=  ^=  |=
		/// </summary>
		Assignment = 10,
		/// <summary>
		/// pseudo operator for comparisons
		/// </summary>
		ParenthesisNotNeeded = 0,
	}
}
