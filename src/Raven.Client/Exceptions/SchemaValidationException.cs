namespace Raven.Client.Exceptions;

public class SchemaValidationException : RavenException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SchemaValidationException"/> class.
    /// </summary>
    /// <param name="message">The message.</param>
    public SchemaValidationException(string message) : base(message)
    {
    }
}
