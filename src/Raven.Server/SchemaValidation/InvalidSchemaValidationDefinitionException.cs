using System;

namespace Raven.Server.SchemaValidation;

internal class InvalidSchemaValidationDefinitionException(string message) : Exception(message);
