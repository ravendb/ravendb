using System;
using System.Collections.Generic;
using System.Text;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class SqlMigrationResult
    {
        public bool Success;

        public string[] Errors;

    }
}
