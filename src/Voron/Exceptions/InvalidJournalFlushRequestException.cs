// -----------------------------------------------------------------------
//  <copyright file="InvalidJournalFlushRequest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Voron.Exceptions
{
    public class InvalidJournalFlushRequestException : Exception
    {

        public InvalidJournalFlushRequestException()
        {
        }

        public InvalidJournalFlushRequestException(string message) : base(message)
        {
        }

        public InvalidJournalFlushRequestException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}