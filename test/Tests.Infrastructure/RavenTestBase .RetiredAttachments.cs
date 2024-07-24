//using System;
//using Raven.Client.Documents.Operations.Backups;
//using Raven.Client.Util;
//using Xunit;

//namespace FastTests
//{
//    public abstract partial class RavenTestBase
//    {
//        public readonly RetiredAttachmentsBase RetiredAttachments;

//        public class RetiredAttachmentsBase
//        {
//            private readonly RavenTestBase _parent;

//            public RetiredAttachmentsBase(RavenTestBase parent)
//            {
//                _parent = parent ?? throw new ArgumentNullException(nameof(parent));
//            }

//            static RetiredAttachmentsBase()
//            {

//            }

//            public IDisposable Initialize(string s3RetireAttachmentsPrefix)
//            {
//                S3Settings settings = _parent.Etl.GetS3Settings(s3RetireAttachmentsPrefix);
//                Assert.NotNull(settings);
//                return new DisposableAction(() =>
//                {
//                    // Do something
//                });
//            }
//        }
//    }
//}
