using System;
using System.Collections.Generic;

namespace Sparrow.Utils.HWT
{
    internal sealed class HashedWheelBucket
    {
        // Used for the linked-list datastructure
        private HashedWheelTimeout _head;
        private HashedWheelTimeout _tail;

        public void AddTimeout(HashedWheelTimeout timeout)
        {
            timeout._bucket = this;
            if (_head == null)
            {
                _head = _tail = timeout;
            }
            else
            {
                _tail._next = timeout;
                timeout._prev = _tail;
                _tail = timeout;
            }
        }

        /// <summary>
        /// Expire all HashedWheelTimeout for the given deadline.
        /// </summary>
        /// <param name="deadline"></param>
        internal void ExpireTimeouts(long deadline)
        {
            HashedWheelTimeout timeout = _head;

            // process all timeouts
            while (timeout != null)
            {
                HashedWheelTimeout next = timeout._next;
                if (timeout._remainingRounds <= 0)
                {
                    next = Remove(timeout);
                    if (timeout._deadline <= deadline)
                    {
                        timeout.Expire();
                    }
                    else
                    {
                        // The timeout was placed into a wrong slot. This should never happen.
                        throw new InvalidOperationException($"timeout.deadline ({timeout._deadline}) > deadline ({deadline})");
                    }
                }
                else if (timeout.Cancelled)
                {
                    next = Remove(timeout);
                }
                else
                {
                    timeout._remainingRounds--;
                }
                timeout = next;
            }
        }

        internal HashedWheelTimeout Remove(HashedWheelTimeout timeout)
        {
            HashedWheelTimeout next = timeout._next;
            // remove timeout that was either processed or cancelled by updating the linked-list
            if (timeout._prev != null)
            {
                timeout._prev._next = next;
            }
            if (timeout._next != null)
            {
                timeout._next._prev = timeout._prev;
            }

            if (timeout == _head)
            {
                // if timeout is also the tail we need to adjust the entry too
                if (timeout == _tail)
                {
                    _tail = null;
                    _head = null;
                }
                else
                {
                    _head = next;
                }
            }
            else if (timeout == _tail)
            {
                // if the timeout is the tail modify the tail to be the prev node.
                _tail = timeout._prev;
            }
            // null out prev, next and bucket to allow for GC.
            timeout._prev = null;
            timeout._next = null;
            timeout._bucket = null;
            timeout._timer.DecreasePendingTimeouts();
            return next;
        }

        internal void ClearTimeouts(ISet<Timeout> set)
        {
            for (; ; )
            {
                HashedWheelTimeout timeout = PollTimeout();
                if (timeout == null)
                {
                    return;
                }
                if (timeout.Expired || timeout.Cancelled)
                {
                    continue;
                }
                set.Add(timeout);
            }
        }

        private HashedWheelTimeout PollTimeout()
        {
            HashedWheelTimeout head = this._head;
            if (head == null)
            {
                return null;
            }
            HashedWheelTimeout next = head._next;
            if (next == null)
            {
                _tail = this._head = null;
            }
            else
            {
                this._head = next;
                next._prev = null;
            }

            // null out prev and next to allow for GC.
            head._next = null;
            head._prev = null;
            head._bucket = null;
            return head;
        }
    }
}
