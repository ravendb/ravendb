﻿using System;

namespace Raven.Client.Exceptions.Changes
{
    public sealed class ChangeProcessingException : RavenException
    {
        public ChangeProcessingException()
        {
        }

        public ChangeProcessingException(string message)
            : base(message)
        {
        }

        public ChangeProcessingException(Exception e)
            : base("Failed to process change.", e)
        {
        }
    }
}