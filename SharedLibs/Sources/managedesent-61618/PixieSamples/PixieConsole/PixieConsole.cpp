//-----------------------------------------------------------------------
// <copyright file="PixieConsole.cpp" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using namespace System;
using namespace System::Collections::Generic;
using namespace System::Diagnostics;
using namespace System::IO;
using namespace Microsoft::Isam::Esent;

// Implements a console for SQL commands. This will read from the input
// and write to the output and error streams.
private ref class SqlConsole
{
public:
	SqlConsole(TextReader ^ const input, TextWriter ^ const output, TextWriter ^ const error);
	~SqlConsole();
	
	// This prompt will be printed when a command is to be read. It defaults to empty.
	property String^ Prompt;	
	
	// Starts the SqlConsole read-evaluate-print loop. This will run until there is no
	// more input.
	void Execute();
		
private:
	// Close the internal SqlConnection
	void CloseSqlConnection();
	
	// Use the SqlConnection to run the specified command
	void ExecuteSqlCommand(String ^ const command);
	
	// Try to run the command as an internal console command. Returns true if the command
	// was a console command.
	bool TryExecuteConsoleCommand(String ^ const command);
	
	// Run for the START TIMER console command
	void StartTimer(array<String^> ^args);
	
	// Run for the STOP TIMER console command
	void StopTimer(array<String^> ^args);

	// Run for the MEMSTATS console command
	void MemStats(array<String^> ^args);

private:
	TextReader ^ const m_input;
	TextWriter ^ const m_output;
	TextWriter ^ const m_error;
			
	SqlConnection ^m_sql;	
	
	// Named timers
	Dictionary<String^, Stopwatch^> ^m_timers;
	
	// Console commands. These are identified by a string prefix (e.g. START TIMER) and
	// the remaining text is split into an array and passed to the delegate.
	Dictionary<String^, Action<array<String^>^>^> ^ const m_consoleCommands;
};

SqlConsole::SqlConsole(TextReader ^ const input, TextWriter ^ const output, TextWriter ^ const error) :
	m_input(input),
	m_output(output),
	m_error(error),
	m_consoleCommands(gcnew Dictionary<String^, Action<array<String^>^>^>())
{
	Prompt = String::Empty;
	
	m_consoleCommands["START TIMER"] = gcnew Action<array<String^>^>(this, &SqlConsole::StartTimer);
	m_consoleCommands["STOP TIMER"] = gcnew Action<array<String^>^>(this, &SqlConsole::StopTimer);
	m_consoleCommands["MEMSTATS"] = gcnew Action<array<String^>^>(this, &SqlConsole::MemStats);
}

// Called when the console is disposed
SqlConsole::~SqlConsole()
{
	CloseSqlConnection();	
}

// Run the read-eval-print loop until there is no more input
void SqlConsole::Execute()
{
	m_sql = Esent::CreateSqlConnection();
	m_timers = gcnew Dictionary<String^, Stopwatch^>();
	
	while(true)
	{
		m_output->Write("{0}", Prompt);
		
		String ^command;		
		if(nullptr == (command = m_input->ReadLine()))
			break;
			
		if (!TryExecuteConsoleCommand(command))
		{
			ExecuteSqlCommand(command);
		}
	}
	CloseSqlConnection();	
}

void SqlConsole::ExecuteSqlCommand(String ^ const command)
{
	try
	{
		m_sql->Execute(command);
	}
	catch(EsentException ^ex)
	{
		m_error->WriteLine(L"*** Caught Exception ***");
		m_error->WriteLine(L"{0}", ex);
		m_error->WriteLine(L"************************");
	}	
}

bool SqlConsole::TryExecuteConsoleCommand(String ^ const command)
{
	for each(String ^consoleCommandPrefix in m_consoleCommands->Keys)
	{
		if (command->StartsWith(consoleCommandPrefix))
		{
			array<String^> ^args = command->Substring(consoleCommandPrefix->Length)->Trim()->Split();
			m_consoleCommands[consoleCommandPrefix](args);
			return true;
		}
	}
	return false;
}

void SqlConsole::StartTimer(array<String^> ^args)
{
	String ^timerName = args->Length > 0 ? args[0] : String::Empty;
	m_timers[timerName] = Stopwatch::StartNew();
}

void SqlConsole::StopTimer(array<String^> ^args)
{
	String ^timerName = args->Length > 0 ? args[0] : String::Empty;
	if (m_timers->ContainsKey(timerName))
	{
		m_timers[timerName]->Stop();
		if (!String::IsNullOrEmpty(timerName))
		{
			m_output->Write("{0}: ", timerName);
		}
		m_output->WriteLine("{0}", m_timers[timerName]->Elapsed);
		m_timers->Remove(timerName);
	}
	else
	{
		m_error->WriteLine("No active timer named {0}", timerName);
	}
}

void SqlConsole::MemStats(array<String^> ^args)
{
  m_output->WriteLine("{0}", GC::GetTotalMemory(false));
}

void SqlConsole::CloseSqlConnection()
{
	delete m_sql;
	m_sql = nullptr;
	
	delete m_timers;
	m_timers = nullptr;
}

int main(array<String ^> ^args)
{
	SqlConsole sqlconsole(Console::In, Console::Out, Console::Error);
	///sqlconsole.Prompt = L"pixie>";
	sqlconsole.Execute();
    return 0;
}
