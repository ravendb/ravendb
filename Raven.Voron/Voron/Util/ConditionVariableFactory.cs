using System;
using System.ComponentModel;
using System.Runtime.InteropServices;

namespace Voron.Util
{
	public class ConditionVariableFactory : IDisposable
	{
		private IntPtr _cs;

		private static int WAIT_TIMEOUT = 258;

		[DllImport("kernel32.dll")]
		static extern void EnterCriticalSection(IntPtr cs);

		[DllImport("kernel32.dll")]
		static extern void InitializeCriticalSection(IntPtr cs);

		[DllImport("kernel32.dll")]
		static extern void LeaveCriticalSection(IntPtr cs);

		[DllImport("kernel32.dll")]
		static extern void DeleteCriticalSection(IntPtr cs);

		[DllImport("kernel32.dll")]
		static extern void InitializeConditionVariable(IntPtr cv);

		[DllImport("kernel32.dll")]
		static extern bool SleepConditionVariableCS(IntPtr cv, IntPtr cs, int ms);
		
		[DllImport("kernel32.dll")]
		static extern void WakeConditionVariable(IntPtr handle);

		public ConditionVariableFactory()
		{
			_cs = Marshal.AllocHGlobal(64); // a bit more than actually needed
			InitializeCriticalSection(_cs);
		}

		public IDisposable EnterCriticalSection()
		{
			EnterCriticalSection(_cs);

			return new DisposableAction(() => LeaveCriticalSection(_cs));
		}

		public ConditionVariable Create()
		{
			return new ConditionVariable(_cs);
		}

		public class ConditionVariable : IDisposable
		{
			private IntPtr _cv;
			private readonly IntPtr _cs;

			public ConditionVariable(IntPtr cs)
			{
				_cs = cs;
				_cv = Marshal.AllocHGlobal(IntPtr.Size);
				InitializeConditionVariable(_cv);
			}

			public bool Wait(int timeout)
			{
				var result = SleepConditionVariableCS(_cv, _cs, timeout);
				if (result == false)
				{
					if(Marshal.GetLastWin32Error() != WAIT_TIMEOUT)
						throw new Win32Exception();
				}
				return result;
			}

			public void Wake()
			{
				WakeConditionVariable(_cv);
			}

			public void Dispose()
			{
				GC.SuppressFinalize(this);
				if (_cv != IntPtr.Zero)
				{
					Marshal.FreeHGlobal(_cv);
					_cv = IntPtr.Zero;
				}
			}

			~ConditionVariable()
			{
				Dispose();
			}
		}

		public void Dispose()
		{
			GC.SuppressFinalize(this);
			if (_cs != IntPtr.Zero)
			{
				DeleteCriticalSection(_cs);
				Marshal.FreeHGlobal(_cs);
				_cs = IntPtr.Zero;
			}
		}

		~ConditionVariableFactory()
		{
			Dispose();
		}
	}


}