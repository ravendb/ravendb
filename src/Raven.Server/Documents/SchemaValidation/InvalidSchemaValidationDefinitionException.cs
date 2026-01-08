using System;

namespace Raven.Server.Documents.SchemaValidation;

internal class InvalidSchemaValidationDefinitionException(string message) : Exception(message);
