using System;
using System.Runtime.Serialization;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Exceptions
{

    [Serializable]
    public class IndexCompilationException : Exception
    {
        //
        // For guidelines regarding the creation of new exception types, see
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
        // and
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
        //

        public IndexCompilationException()
        {
        }

        public IndexCompilationException(string message)
            : base(message)
        {
        }

        public IndexCompilationException(string message, Exception inner)
            : base(message, inner)
        {
        }

        /// <summary>
        /// Indicates which property caused error (Maps, Reduce).
        /// </summary>
        public string IndexDefinitionProperty { get; set; }

        /// <summary>
        /// Value of a problematic property.
        /// </summary>
        public string ProblematicText { get; set; }
        protected IndexCompilationException(
            SerializationInfo info,
            StreamingContext context)
            : base(info, context)
        {
        }

        public override string ToString()
        {
            return this.ExceptionToString(description =>
                                          description.AppendFormat(
                                              ", IndexDefinitionProperty='{0}', ProblematicText='{1}'",
                                              IndexDefinitionProperty,
                                              ProblematicText));
        }
    }
}
