using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Raven.Client.Exceptions.SchemaValidation;
using Raven.Server.Documents.SchemaValidation.ErrorMessage;

namespace Raven.Server.Documents.SchemaValidation;

public class SchemaValidationContext
{
    private readonly long _timeoutAt;
    private readonly TimeSpan _timeout;
    private int _currentDepth;
    private readonly int _maxDepth;

    public ErrorBuilder ErrorBuilder
    {
        init;
        get => _withError ? field : null;
    }

    private bool _withError = true;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SchemaValidationContext(SchemaValidatorSettings configuration)
    {
        _maxDepth = configuration.MaxDepth;
        _timeout = configuration.ValidationTimeout;
        _timeoutAt = Stopwatch.GetTimestamp() + (long)(_timeout.TotalMilliseconds * (Stopwatch.Frequency / 1000.0));
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void StepIn(string property)
    {
        ErrorBuilder?.Path.StepIn(property);
        ++_currentDepth;
        CheckMaxDepthAndThrow();
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void StepIn(int index)
    {
        ErrorBuilder?.Path.StepIn(index);
        ++_currentDepth;
        CheckMaxDepthAndThrow();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void StepOut()
    {
        ErrorBuilder?.Path.StepOut();
        --_currentDepth;
    }

    private void CheckMaxDepthAndThrow()
    {
        if (_currentDepth >= _maxDepth)
            throw new SchemaValidationException($"Maximum validation path depth of {_maxDepth} exceeded.");
    }

    public void CheckTimeoutAndThrow()
    {
        if (Stopwatch.GetTimestamp() > _timeoutAt)
            throw new SchemaValidationException($"Schema validation timed out after {_timeout.TotalMilliseconds}ms.");
    }
    
    public WithoutErrorScope WithoutCollectingErrors()
    {
        return new WithoutErrorScope(this);
    }
    
    public struct WithoutErrorScope : IDisposable
    {
        private SchemaValidationContext _context;
        private readonly bool _previousWithError;

        public WithoutErrorScope(SchemaValidationContext context)
        {
            _context = context;
            _previousWithError = context._withError;
            _context._withError = false;
        }

        public void Dispose()
        {
            _context._withError = _previousWithError;
        }
    }
}
