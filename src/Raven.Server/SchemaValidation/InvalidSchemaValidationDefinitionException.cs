using System;

namespace Raven.Server.Exceptions.SchemaValidation;

internal class InvalidSchemaValidationDefinitionException(string message) : Exception(message);
