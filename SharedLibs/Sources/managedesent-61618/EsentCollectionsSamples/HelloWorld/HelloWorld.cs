//-----------------------------------------------------------------------
// <copyright file="HelloWorld.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace PersistentDictionarySample
{
    using System;
    using Microsoft.Isam.Esent.Collections.Generic;

    /// <summary>
    /// A simple 'Hello World' program that uses a PersistentDictionary.
    /// This remembers the names of its previous users.
    /// </summary>
    public static class HelloWorld
    {
        /// <summary>
        /// Ask the user for their first name and see if we remember 
        /// their last name.
        /// </summary>
        public static void Main()
        {
            PersistentDictionary<string, string> dictionary = new PersistentDictionary<string, string>("Names");
            Console.WriteLine("What is your first name?");
            string firstName = Console.ReadLine();
            if (dictionary.ContainsKey(firstName))
            {
                Console.WriteLine("Welcome back {0} {1}", firstName, dictionary[firstName]);
            }
            else
            {
                Console.WriteLine("I don't know you, {0}. What is your last name?", firstName);
                dictionary[firstName] = Console.ReadLine();
            }
        }
    }
}
