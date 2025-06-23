﻿using System;
using System.IO;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Storage
{
    public class VeryBig : FastTests.Voron.StorageTest
    {
        public VeryBig(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanGrowBeyondInitialSize()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("test");
                tx.Commit();
            }

            var buffer = new byte[1024 * 512];
            new Random().NextBytes(buffer);

            for (int i = 0; i < 20; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("test");
                    for (int j = 0; j < 12; j++)
                    {
                        tree.Add(string.Format("{0:000}-{1:000}", j, i), new MemoryStream(buffer));
                    }
                    tx.Commit();
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanGrowBeyondInitialSize_Root()
        {
            var buffer = new byte[1024 * 512];
            new Random().NextBytes(buffer);

            for (int i = 0; i < 20; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("test");
                    for (int j = 0; j < 12; j++)
                    {
                        tree.Add(string.Format("{0:000}-{1:000}", j, i), new MemoryStream(buffer));
                    }
                    tx.Commit();
                }
            }
        }
        [RavenFact(RavenTestCategory.Voron)]
        public void CanGrowBeyondInitialSize_WithAnotherTree()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("test");
                tx.Commit();
            }
            var buffer = new byte[1024 * 512];
            new Random().NextBytes(buffer);

            for (int i = 0; i < 20; i++)
            {
                using (var tx = Env.WriteTransaction())
                {

                    var tree = tx.CreateTree("test");
                    for (int j = 0; j < 12; j++)
                    {
                        tree.Add(string.Format("{0:000}-{1:000}", j, i), new MemoryStream(buffer));
                    }
                    tx.Commit();
                }
            }
        }
    }
}
