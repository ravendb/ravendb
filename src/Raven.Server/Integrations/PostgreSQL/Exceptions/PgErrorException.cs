using System;

namespace Raven.Server.Integrations.PostgreSQL.Exceptions
{
    public class PgErrorException : Exception
    {
        public string ErrorCode;

        /// <summary>
        /// Creates an Postgres exception to be sent back to the client
        /// </summary>
        /// <param name="errorCode">A Postgres error code (SqlState). See <see cref="PgErrorCodes"/></param>
        /// <param name="errorMessage">Error message</param>
        /// <returns>ErrorResponse message</returns>
        public PgErrorException(string errorCode, string errorMessage) : base(errorMessage)
        {
            ErrorCode = errorCode;
        }
    }
}
