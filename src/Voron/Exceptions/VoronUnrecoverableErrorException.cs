// -----------------------------------------------------------------------
//  <copyright file="VoronUnrecoverableErrorException.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.ExceptionServices;
using Voron.Impl;

namespace Voron.Exceptions
{
    public class VoronUnrecoverableErrorException : Exception
    {
        public static void Raise(LowLevelTransaction tx, string message)
        {
            try
            {
                var lastTxState = tx.GetTxState();
                tx.MarkTransactionAsFailed();
                throw new VoronUnrecoverableErrorException($"{message}. LastTxState: {lastTxState}"
                    + Environment.NewLine + " @ " + tx.Environment.Options.DataPager.FileName.FullPath);
            }
            catch (Exception e)
            {
                tx.Environment.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                throw;
            }
        }
        public static void Raise(StorageEnvironment env, string message)
        {
            try
            {
                throw new VoronUnrecoverableErrorException(message + Environment.NewLine + " @ " + env.Options.DataPager.FileName.FullPath);
            }
            catch (Exception e)
            {
                env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                throw;
            }
        }

        public static void Raise(StorageEnvironmentOptions options, string message)
        {
            try
            {
                throw new VoronUnrecoverableErrorException(message
                    + Environment.NewLine + " @ " + options.DataPager.FileName.FullPath);
            }
            catch (Exception e)
            {
                options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                throw;
            }
        }

        public static void Raise(StorageEnvironment env, string message, Exception inner)
        {
            try
            {
                throw new VoronUnrecoverableErrorException(message
                    + Environment.NewLine + " @ " + env.Options.DataPager.FileName.FullPath, inner);
            }
            catch (Exception e)
            {
                env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                throw;
            }
        }
        
        public static void Raise(StorageEnvironmentOptions options, string message, Exception inner)
        {
            try
            {
                throw new VoronUnrecoverableErrorException(message
                    + Environment.NewLine + " @ " + options.DataPager.FileName.FullPath, inner);
            }
            catch (Exception e)
            {
                options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                throw;
            }
        }


        public static void Raise(string message, Exception inner)
        {
            throw new VoronUnrecoverableErrorException(message, inner);
        }

        protected VoronUnrecoverableErrorException(string message)
            : base(message)
        {
        }

        private VoronUnrecoverableErrorException(string message, Exception inner)
            : base(message, inner)
        {
        }

    }
}
