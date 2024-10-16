﻿using System;

namespace Raven.Client.Exceptions
{
    public sealed class CompareExchangeKeyTooBigException : RavenException
    {
        public CompareExchangeKeyTooBigException(string message)
            : base(message)
        {
        }
        public CompareExchangeKeyTooBigException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
