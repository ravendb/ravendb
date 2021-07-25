using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Tokenizers;
using Xunit;
using Xunit.Abstractions;

namespace CoraxTests
{
    public class IndexTransactionTests : IndexStorageTests
    {
        public IndexTransactionTests(ITestOutputHelper output) : base(output)
        {}

        [Fact]
        public void InitializeIndex()
        {
            Env.Initialize();
        }
    }
}
