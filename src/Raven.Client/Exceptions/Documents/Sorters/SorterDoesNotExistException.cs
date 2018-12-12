using System;

namespace Raven.Client.Exceptions.Documents.Sorters
{
    public class SorterDoesNotExistException : RavenException
    {
        public SorterDoesNotExistException()
        {
        }

        public SorterDoesNotExistException(string message) : base(message)
        {
        }

        public SorterDoesNotExistException(string message, Exception inner) : base(message, inner)
        {
        }

        public static SorterDoesNotExistException ThrowFor(string sorterName)
        {
            throw new SorterDoesNotExistException($"There is no sorter with '{sorterName}' name.");
        }
    }
}
