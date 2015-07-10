using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace Sparrow.Tests
{
    public class BitVectorsTest
    {
        public static IEnumerable<object[]> VectorSize
        {
            get
            {
                return new[]
				{
					new object[] {1},
					new object[] {4},
                    new object[] {7},
					new object[] {8},
                    new object[] {9},
                    new object[] {16},
                    new object[] {17},
                    new object[] {65},
                    new object[] {513},
				};
            }
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void Construction(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            
            Assert.Equal(vectorSize, vector.Count);           
            for (int i = 0; i < vectorSize; i++)
                Assert.False(vector[i]);

            vector = new BitVector(vectorSize);

            Assert.Equal(vectorSize, vector.Count);
            for (int i = 0; i < vectorSize; i++)
                Assert.False(vector[i]);
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void SetByIndex(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            for (int i = 0; i < vectorSize; i++)
            {
                Assert.False(vector[i]);
                vector[i] = true;

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector[j] == (i == j));

                Assert.True(vector[i]);
                vector[i] = false;

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector[j] == (i != j));
            }
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void SetByMethod(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            for (int i = 0; i < vectorSize; i++)
            {
                Assert.False(vector.Get(i));
                vector.Set(i, true);

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector.Get(j) == (i == j));

                Assert.True(vector[i]);
                vector.Set(i, false);

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector.Get(j) == (i != j));
            }
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void FlipBit(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            for (int i = 0; i < vectorSize; i++)
            {
                Assert.False(vector[i]);
                vector.Flip(i);

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector[j] == (i == j));

                Assert.True(vector[i]);
                vector.Flip(i);

                for (int j = 0; j < vectorSize; j++)
                    Assert.True(vector[j] == (i != j));
            }
        }

        [Theory]
        [PropertyData("VectorSize")]
        public void FillClear(int vectorSize)
        {
            var vector = BitVector.OfLength(vectorSize);
            vector.Fill(true);
            for (int i = 0; i < vectorSize; i++)
                Assert.True(vector[i]);

            vector.Clear();
            for (int i = 0; i < vectorSize; i++)
                Assert.False(vector[i]);
        }
    }
}
