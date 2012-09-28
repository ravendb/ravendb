using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace Jint.Tests {
    /// <summary>
    /// Summary description for SunSpider
    /// </summary>
    [TestClass]
    public class SunSpider {

        private static void ExecuteSunSpiderScript(string scriptName)
        {
            const string prefix = "Jint.Tests.SunSpider.";
            var script = prefix + scriptName;

            var assembly = Assembly.GetExecutingAssembly();
            var program = new StreamReader(assembly.GetManifestResourceStream(script)).ReadToEnd();

            var jint = new JintEngine(Options.Ecmascript5); // The SunSpider scripts doesn't work with strict mode
            var sw = new Stopwatch();
            sw.Start();

            jint.Run(program);

            Console.WriteLine(sw.Elapsed);
        }

                

        [TestMethod]
        public void ShouldRun3DCube()
        {
            ExecuteSunSpiderScript("3d-cube.js");
        }

        [TestMethod]
        public void ShouldRun3DMorph()
        {
            ExecuteSunSpiderScript("3d-morph.js");
        }

        [TestMethod]
        public void ShouldRun3DRaytrace()
        {
            ExecuteSunSpiderScript("3d-raytrace.js");
        }

        [TestMethod]
        public void ShouldRunAccessBinaryTrees()
        {
            ExecuteSunSpiderScript("access-binary-trees.js");
        }

        [TestMethod]
        public void ShouldRunAccessFannkuch()
        {
            ExecuteSunSpiderScript("access-fannkuch.js");
        }

        [TestMethod]
        public void ShouldRunAccessNbody()
        {
            ExecuteSunSpiderScript("access-nbody.js");
        }

        [TestMethod]
        public void ShouldRunAccessNsieve()
        {
            ExecuteSunSpiderScript("access-nsieve.js");
        }

        [TestMethod]
        public void ShouldRunBitops3BitsInByte()
        {
            ExecuteSunSpiderScript("bitops-3bit-bits-in-byte.js");
        }

        [TestMethod]
        public void ShouldRunBitopsBitsInByte()
        {
            ExecuteSunSpiderScript("bitops-bits-in-byte.js");
        }

        [TestMethod]
        public void ShouldRunBitopsBitwiseAnd()
        {
            ExecuteSunSpiderScript("bitops-bitwise-and.js");
        }

        [TestMethod]
        public void ShouldRunBitopsNsieveBits()
        {
            ExecuteSunSpiderScript("bitops-nsieve-bits.js");
        }

        [TestMethod]
        public void ShouldRunControlflowRecurise()
        {
            ExecuteSunSpiderScript("controlflow-recursive.js");
        }

        [TestMethod]
        public void ShouldRunCryptoAes()
        {
            ExecuteSunSpiderScript("crypto-aes.js");
        }

        [TestMethod]
        public void ShouldRunCrypotMd5()
        {
            ExecuteSunSpiderScript("crypto-md5.js");
        }
        
        [TestMethod]
        public void ShouldRunCruptoSha1()
        {
            ExecuteSunSpiderScript("crypto-sha1.js");
        }

        [TestMethod]
        public void ShouldRunDateFormatTofte()
        {
            ExecuteSunSpiderScript("date-format-tofte.js");
        }

        [TestMethod]
        public void ShouldRunDateFormatXparb()
        {
            ExecuteSunSpiderScript("date-format-xparb.js");
        }

        [TestMethod]
        public void ShouldRunMathCrodic()
        {
            ExecuteSunSpiderScript("math-cordic.js");
        }

        [TestMethod]
        public void ShouldRunMathPartialSums()
        {
            ExecuteSunSpiderScript("math-partial-sums.js");
        }

        [TestMethod]
        public void ShouldRunMathSpecialNorm()
        {
            ExecuteSunSpiderScript("math-spectral-norm.js");
        }

        [TestMethod]
        public void ShouldRunRegexpDna()
        {
            ExecuteSunSpiderScript("regexp-dna.js");
        }

        [TestMethod]
        public void ShouldRunStringBase64()
        {
            ExecuteSunSpiderScript("string-base64.js");
        }
        

        [TestMethod]
        public void ShouldRunStinFasta()
        {
            ExecuteSunSpiderScript("string-fasta.js");
        }

                [TestMethod]
        public void ShouldRunStringTagcloud()
        {
            ExecuteSunSpiderScript("string-tagcloud.js");
        }

                [TestMethod]
        public void ShouldRunStringUnpackCode()
        {
            ExecuteSunSpiderScript("string-unpack-code.js");
        }

        [TestMethod]
        public void ShouldRunStringValidateInput()
        {
            ExecuteSunSpiderScript("string-validate-input.js");
        }

    }
}
