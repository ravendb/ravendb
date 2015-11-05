// -----------------------------------------------------------------------
//  <copyright file="ConsoleHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Smuggler.Helpers
{
    public static class ConsoleHelper
    {
        public static void WriteLineWithColor(ConsoleColor color, string message, params object[] args)
        {
            var previousColor = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine(message, args);
            Console.ForegroundColor = previousColor;
        }
    }
}
