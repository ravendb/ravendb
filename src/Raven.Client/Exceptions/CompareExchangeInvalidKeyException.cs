using System;

namespace Raven.Client.Exceptions;

/// <summary>
/// Exception thrown when an invalid key is used in a compare exchange operation.
/// </summary>
public sealed class CompareExchangeInvalidKeyException : RavenException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CompareExchangeInvalidKeyException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public CompareExchangeInvalidKeyException(string message)
        : base(message)
    {
    }
    
    /// <summary>
    /// Initializes a new instance of the <see cref="CompareExchangeInvalidKeyException"/> class with a specified error message and a reference to the inner exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="inner">The exception that is the cause of the current exception.</param>
    public CompareExchangeInvalidKeyException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
