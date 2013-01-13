//-----------------------------------------------------------------------
// <copyright file="IndexDefinitionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the IndexDefinition class
    /// </summary>
    [TestClass]
    public class IndexDefinitionTests
    {
        [TestMethod]
        [Priority(1)]
        public void VerifyConstructorSetsName()
        {
            var indexdefinition = new IndexDefinition("index");
            Assert.AreEqual("index", indexdefinition.Name);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyAsPrimarySetsUnique()
        {
            var indexdefinition = new IndexDefinition("index").AsPrimary();
            Assert.IsTrue(indexdefinition.IsUnique);
        }
    }
}
