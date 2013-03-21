//-----------------------------------------------------------------------
// <copyright file="Session.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Globalization;

    /// <summary>
    /// A class that encapsulates a JET_SESID in a disposable object.
    /// </summary>
    public class Session : EsentResource
    {
        /// <summary>
        /// The underlying JET_SESID.
        /// </summary>
        private JET_SESID sesid;

        /// <summary>
        /// Initializes a new instance of the Session class. A new
        /// JET_SESSION is allocated from the given instance.
        /// </summary>
        /// <param name="instance">The instance to start the session in.</param>
        public Session(JET_INSTANCE instance)
        {
            Api.JetBeginSession(instance, out this.sesid, null, null);
            this.ResourceWasAllocated();
        }

        /// <summary>
        /// Gets the JET_SESID that this session contains.
        /// </summary>
        public JET_SESID JetSesid
        {
            get
            {
                this.CheckObjectIsNotDisposed();
                return this.sesid;
            }
        }

        /// <summary>
        /// Implicit conversion operator from a Session to a JET_SESID. This
        /// allows a Session to be used with APIs which expect a JET_SESID.
        /// </summary>
        /// <param name="session">The session to convert.</param>
        /// <returns>The JET_SESID of the session.</returns>
        public static implicit operator JET_SESID(Session session)
        {
            return session.JetSesid;
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="Session"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="Session"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "Session (0x{0:x})", this.sesid.Value);
        }

        /// <summary>
        /// Terminate the session.
        /// </summary>
        public void End()
        {
            this.CheckObjectIsNotDisposed();
            this.ReleaseResource();
        }

        /// <summary>
        /// Free the underlying JET_SESID.
        /// </summary>
        protected override void ReleaseResource()
        {
            if (JET_SESID.Nil != this.sesid)
            {
                Api.JetEndSession(this.JetSesid, EndSessionGrbit.None);
            }

            this.sesid = JET_SESID.Nil;
            this.ResourceWasReleased();
        }
    }
}