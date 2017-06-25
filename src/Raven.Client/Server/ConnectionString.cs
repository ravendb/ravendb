using System;
using System.Collections.Generic;

namespace Raven.Client.Server
{
    public abstract class ConnectionString
    {
        public string Name { get; set; }

        public bool Validate(ref List<string> errors)
        {
            if (errors == null)
                throw new ArgumentNullException(nameof(errors));

            var count = errors.Count;

            ValidateImpl(ref errors);

            return count == errors.Count;
        }

        public abstract ConnectionStringType Type { get; }

        protected abstract void ValidateImpl(ref List<string> errors);
    }

    public enum ConnectionStringType
    {
        Raven,
        Sql
    }
}