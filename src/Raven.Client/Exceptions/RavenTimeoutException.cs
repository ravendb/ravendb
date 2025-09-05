using System;

namespace Raven.Client.Exceptions;

/// <summary>
/// Exception thrown when a RavenDB operation times out.
/// </summary>
public sealed class RavenTimeoutException : RavenException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RavenTimeoutException"/> class.
    /// </summary>
    public RavenTimeoutException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RavenTimeoutException"/> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public RavenTimeoutException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RavenTimeoutException"/> class with a specified error message and a reference to the inner exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="inner">The exception that is the cause of the current exception.</param>
    public RavenTimeoutException(string message, Exception inner)
        : base(message, inner)
    {
    }

    /// <summary>
    /// Gets or sets a value indicating whether the operation should fail immediately on timeout.
    /// </summary>
    public bool FailImmediately;
}
