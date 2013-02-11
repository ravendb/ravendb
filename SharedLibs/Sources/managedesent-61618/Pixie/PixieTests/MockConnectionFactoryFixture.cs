//-----------------------------------------------------------------------
// <copyright file="MockConnectionFactoryFixture.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Rhino.Mocks;

namespace PixieTests
{
    /// <summary>
    /// Test the Esent methods by overriding the ConnectionManager singleton.
    /// </summary>
    [TestClass]
    public class MockConnectionFactoryFixture
    {
        /// <summary>
        /// Mock object repository.
        /// </summary>
        private MockRepository mocks;

        /// <summary>
        /// The mock ConnectionManager.
        /// </summary>
        private IConnectionManager mockConnectionManager;

        /// <summary>
        /// The fake connection to be returned.
        /// </summary>
        private Connection dummyConnection;

        /// <summary>
        /// Setup the mock ConnectionManager and SqlConnection.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.mocks = new MockRepository();
            this.mockConnectionManager = this.mocks.StrictMock<IConnectionManager>();
            this.dummyConnection = this.mocks.Stub<Connection>();
            Dependencies.Container.RegisterInstance<IConnectionManager>(this.mockConnectionManager);
        }

        /// <summary>
        /// Resore the overridden registry values.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            Dependencies.InitializeContainer();
        }

        /// <summary>
        /// Verify that Esent.CreateDatabase uses None as its default creation mode.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void EsentCreateDatabaseUsesCreationModeNoneAsDefault()
        {
            Expect.Call(this.mockConnectionManager.CreateDatabase("mydatabase.edb", DatabaseCreationMode.None)).Return(this.dummyConnection);
            this.mocks.ReplayAll();
            Esent.CreateDatabase("mydatabase.edb");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// Verify that Esent.CreateDatabase passes the specified creation mode to the ConnectionManager.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void EsentCreateDatabasePassesCreationModeToConnectionFactory()
        {
            Expect.Call(this.mockConnectionManager.CreateDatabase("mydatabase.edb", DatabaseCreationMode.OverwriteExisting)).Return(this.dummyConnection);
            this.mocks.ReplayAll();
            Esent.CreateDatabase("mydatabase.edb", DatabaseCreationMode.OverwriteExisting);
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// Verify that Esent.OpenDatabase uses ReadWrite as its default open mode.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void EsentOpenDatabaseUsesOpenModeReadWriteAsDefault()
        {
            Expect.Call(this.mockConnectionManager.AttachDatabase("mydatabase.edb", DatabaseOpenMode.ReadWrite)).Return(this.dummyConnection);
            this.mocks.ReplayAll();
            Esent.OpenDatabase("mydatabase.edb");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// Verify that Esent.OpenDatabase passes the specified open mode to the connection factory.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void EsentOpenDatabasePassesOpenModeToConnectionFactory()
        {
            Expect.Call(this.mockConnectionManager.AttachDatabase("mydatabase.edb", DatabaseOpenMode.ReadOnly)).Return(this.dummyConnection);
            this.mocks.ReplayAll();
            Esent.OpenDatabase("mydatabase.edb", DatabaseOpenMode.ReadOnly);
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// Verify that ISqlImpl.CreateDatabase calls ConnectionManager.CreateDatabase
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void SqlImplCreateDatabaseCallsConnectionFactory()
        {
            Expect.Call(this.mockConnectionManager.CreateDatabase("sqldatabase.edb", DatabaseCreationMode.None)).Return(this.dummyConnection);
            this.mocks.ReplayAll();
            var sqlimpl = new SqlImplBase();
            sqlimpl.CreateDatabase("sqldatabase.edb");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// Verify that ISqlImpl.AttachDatabase calls ConnectionManager.AttachDatabase
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void SqlImplAttachDatabaseCallsConnectionFactory()
        {
            Expect.Call(this.mockConnectionManager.AttachDatabase("sqldatabase.edb", DatabaseOpenMode.ReadWrite)).Return(this.dummyConnection);
            this.mocks.ReplayAll();
            var sqlimpl = new SqlImplBase();
            sqlimpl.AttachDatabase("sqldatabase.edb");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// Verify that CREATE DATABASE calls ConnectionManager.CreateDatabase
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void SqlCreateDatabaseCallsConnectionFactory()
        {
            Expect.Call(this.mockConnectionManager.CreateDatabase("sql.edb", DatabaseCreationMode.None)).Return(this.dummyConnection);
            this.mocks.ReplayAll();
            SqlConnection sql = Esent.CreateSqlConnection();
            sql.Execute("CREATE DATABASE 'sql.edb'");
            this.mocks.VerifyAll();
        }

        /// <summary>
        /// Verify that ATTACH DATABASE calls ConnectionManager.AttachDatabase
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void SqlAttachDatabaseCallsConnectionFactory()
        {
            Expect.Call(this.mockConnectionManager.AttachDatabase("sql.edb", DatabaseOpenMode.ReadWrite)).Return(this.dummyConnection);
            this.mocks.ReplayAll();
            SqlConnection sql = Esent.CreateSqlConnection();
            sql.Execute("ATTACH DATABASE 'sql.edb'");
            this.mocks.VerifyAll();
        }
    }
}
