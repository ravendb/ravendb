//-----------------------------------------------------------------------
// <copyright file="JetParameterTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.Isam.Esent;
using Microsoft.Isam.Esent.Interop;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the JetParameter class.
    /// </summary>
    [TestClass]
    public class JetParameterTests
    {
        private Instance instance;

        [TestInitialize]
        public void Setup()
        {
            this.instance = new Instance("JetParameterTests");
        }

        [TestCleanup]
        public void Teardown()
        {
            Api.JetTerm(this.instance);
        }

        [TestMethod]
        [Priority(1)]
        public void SetJetParameterAsString()
        {
            var jetparam = new JetParameter(JET_param.BaseName, "abc");
            jetparam.SetParameter(this.instance);

            var parameters = new InstanceParameters(this.instance);
            Assert.AreEqual("abc", parameters.BaseName);
        }

        [TestMethod]
        [Priority(1)]
        public void SetJetParameterAsInteger()
        {
            var jetparam = new JetParameter(JET_param.MaxVerPages, 3000);
            jetparam.SetParameter(this.instance);

            var parameters = new InstanceParameters(this.instance);
            Assert.AreEqual(3000, parameters.MaxVerPages);
        }
    }
}