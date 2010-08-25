using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.Indexes
{
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
        /// <<  >>
        /// </summary>
        Shift = 110,    
        /// <summary>
        /// <  >  <=  >=  is  as
        /// </summary>
        RelationalAndTypeTesting = 100,             
        /// <summary>
        /// ==  !=
        /// </summary>
        Equality = 90,               
        /// <summary>
        /// &
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
        /// && (AndAlso in VB)
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
        /// =  *=  /=  %=  +=  -=  <<=  >>=  &=  ^=  |=
        /// </summary>
        Assignment = 10,

        /// <summary>
        /// pseudo operator for comparisons
        /// </summary>
        ParenthesisNotNeeded = 0,
    }

    public static class ExpressionOperatorPrecedenceExtension
    {
        public static bool NeedsParenthesisFor(this ExpressionOperatorPrecedence outer, ExpressionOperatorPrecedence inner)
        {
            if (outer == ExpressionOperatorPrecedence.ParenthesisNotNeeded || inner == ExpressionOperatorPrecedence.ParenthesisNotNeeded)
                return false;

            return outer > inner;
        }
    }
}
