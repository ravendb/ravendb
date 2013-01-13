//-----------------------------------------------------------------------
// <copyright file="Parser.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using gppg;
using Microsoft.Practices.Unity;

namespace Microsoft.Isam.Esent.Sql.Parsing
{
    /// <summary>
    /// Parser implementation.
    /// </summary>
    internal partial class Parser : ShiftReduceParser<ValueType, LexLocation>, SqlConnection
    {
        /// <summary>
        /// Initializes a new instance of the Parser class.
        /// </summary>
        /// <param name="sqlImpl">The object that implements the SQL commands.</param>
        [InjectionConstructor]
        public Parser(ISqlImpl sqlImpl)
        {
            this.SqlImplementation = sqlImpl;
            this.Scanner = new Scanner();
            this.Tracer = new Tracer("SqlParser", "SQL parser for ESENT", "SQL parser");
        }

        /// <summary>
        /// Gets or sets the object that executes the parsed SQL commands.
        /// </summary>
        private ISqlImpl SqlImplementation { get; set; }

        /// <summary>
        /// Gets or sets the object that parses the SQL commands.
        /// </summary>
        private Scanner Scanner
        {
            get
            {
                return (Scanner)this.scanner;
            }

            set
            {
                this.scanner = value;
            }
        }

        /// <summary>
        /// Gets or sets the Tracer object for this instance.
        /// </summary>
        private Tracer Tracer { get; set; }

        /// <summary>
        /// Parse and execute a SQL command.
        /// </summary>
        /// <param name="command">The command to execute.</param>
        public void Execute(string command)
        {
            this.Scanner.SetSource(command);
            this.Parse();
        }

        /// <summary>
        /// Frees the Esent resources used by the parser.
        /// </summary>
        public void Dispose()
        {
            this.SqlImplementation.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}