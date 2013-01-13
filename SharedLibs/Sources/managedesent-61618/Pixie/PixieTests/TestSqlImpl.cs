//-----------------------------------------------------------------------
// <copyright file="TestSqlImpl.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.Isam.Esent;

namespace PixieTests
{
    /// <summary>
    /// Inherit from the ISqlImpl class and provide a way to set the 
    /// connection.
    /// </summary>
    internal class TestSqlImpl : SqlImplBase
    {
        public TestSqlImpl()
            : base()
        {
        }

        internal void FTO_SetConnection(Connection connection)
        {
            this.Connection = connection;
        }
    }
}